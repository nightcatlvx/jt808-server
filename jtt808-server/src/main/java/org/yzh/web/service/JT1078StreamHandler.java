package org.yzh.web.service;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JT1078StreamHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final InetSocketAddress zlmAddress;

    private Socket tcpSocket;
    private OutputStream out;

    // 用于累加一帧完整视频的数据
    private final ByteArrayOutputStream frameBuffer = new ByteArrayOutputStream(16384);

    private int ssrc;
    private int rtpTimestamp;
    private byte pt = 98; // 负载类型，通常 98 代表 H.264
    private int rtpSeq = 0;

    public JT1078StreamHandler(InetSocketAddress zlmAddress) {
        this.zlmAddress = zlmAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("==== JT1078设备连接 remote={} ====", ctx.channel().remoteAddress());
        connectZlm();
    }

    private void connectZlm() {
        try {
            log.info("正在连接ZLM TCP {}...", zlmAddress);
            tcpSocket = new Socket(zlmAddress.getAddress(), zlmAddress.getPort());
            tcpSocket.setTcpNoDelay(true);
            out = tcpSocket.getOutputStream();
            log.info("ZLM TCP连接成功");
        } catch (Exception e) {
            log.error("连接ZLM失败: {}", e.getMessage());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if (msg.readableBytes() < 30) return; // 基础头长度 30 字节

        msg.markReaderIndex();
        int mark = msg.readInt(); // 0x30313633
        byte mpt = msg.readByte();
        byte m = (byte) ((mpt >> 7) & 0x01);
        int frameSsrc = msg.readInt();

        // 关键点：[15] 字节包含数据类型和分包标记
        byte subPkt = msg.readByte();
        byte dataType = (byte) ((subPkt >> 4) & 0x0F); // 高4位: 0=I, 1=P, 2=B, 3=音频
        byte flag = (byte) (subPkt & 0x0F);             // 低4位: 0=原子包, 1=首包, 2=末包, 3=中间包

        // ==========================================
        // 核心修复：过滤非视频数据 (避免干扰 H.264 解析)
        // ==========================================
        if (dataType != 0 && dataType != 1 && dataType != 2) {
            log.debug("忽略非视频数据: dataType={}, ssrc=0x{}", dataType, Integer.toHexString(frameSsrc));
            return;
        }

        int tsHigh = msg.readInt();
        int tsLow = msg.readInt();

        // 跳过不关注的字段（24-29字节）
        msg.skipBytes(6);

        int payloadLen = msg.readableBytes();

        // 处理 SSRC 和 PT
        if (ssrc == 0) {
            ssrc = frameSsrc;
            pt = (byte) (mpt & 0x7F);
        }

        // ==========================================
        // 核心逻辑：时间戳同步与分包组帧
        // ==========================================
        // 只有在收到一帧的“开始”（原子包或首包）时，才更新 RTP 时间戳
        if (flag == 0 || flag == 1) {
            // JT1078 毫秒转 RTP 90kHz 时钟
            rtpTimestamp = (int) (tsLow * 90L);
        }

        byte[] payload = new byte[payloadLen];
        msg.readBytes(payload);
        frameBuffer.write(payload);

        // 如果是原子包(0)或末包(2)，说明一帧收齐了，发送给 ZLM
        if (flag == 0 || flag == 2) {
            flushFrame();
        }
    }

    private void flushFrame() {
        byte[] fullFrame = frameBuffer.toByteArray();
        frameBuffer.reset();

        if (fullFrame.length == 0 || out == null) return;

        try {
            // 将视频帧切分为符合 RTP 标准的包发送给 ZLM
            int offset = 0;
            while (offset < fullFrame.length) {
                int len = Math.min(fullFrame.length - offset, 1400);
                boolean isLastPkt = (offset + len >= fullFrame.length);

                // 构造 RTP 头 (12 字节)
                ByteBuf rtp = Unpooled.buffer(12 + len);
                rtp.writeByte(0x80);
                rtp.writeByte(isLastPkt ? (pt | 0x80) : pt);
                rtp.writeShort(++rtpSeq & 0xFFFF);
                rtp.writeInt(rtpTimestamp);
                rtp.writeInt(ssrc);
                rtp.writeBytes(fullFrame, offset, len);

                // 发送给 ZLM (前面加 2 字节长度前缀，符合 ZLM 的 RTP over TCP 格式)
                byte[] rtpBytes = new byte[rtp.readableBytes()];
                rtp.readBytes(rtpBytes);

                out.write((rtpBytes.length >> 8) & 0xFF);
                out.write(rtpBytes.length & 0xFF);
                out.write(rtpBytes);

                offset += len;
            }
            out.flush();
        } catch (Exception e) {
            log.error("推送至ZLM异常: {}", e.getMessage());
            closeZlm();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("==== JT1078设备断开 remote={} ====", ctx.channel().remoteAddress());
        frameBuffer.reset();
        ctx.channel().eventLoop().schedule(this::closeZlm, 2, TimeUnit.SECONDS);
    }

    private void closeZlm() {
        try {
            if (out != null) out.close();
            if (tcpSocket != null) tcpSocket.close();
        } catch (Exception ignored) {}
        out = null;
        tcpSocket = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("JT1078连接异常: {}", cause.getMessage());
        ctx.close();
    }
}