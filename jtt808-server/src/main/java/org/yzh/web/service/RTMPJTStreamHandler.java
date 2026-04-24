package org.yzh.web.service;

import io.github.yezhihao.netmc.session.Session;
import io.github.yezhihao.netmc.session.SessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.yzh.protocol.commons.JT1078;
import org.yzh.protocol.t1078.T9105;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * JT/T 1078 视频帧接收处理器（RTMP 推流版）
 *
 * 与 JT1078StreamHandler 的区别：
 *  - 旧版：组帧后封装 RTP，以 RFC4571 TCP 推给 ZLM 的 rtp_proxy 端口
 *  - 新版：组帧后通过 RtmpPusher 推 RTMP，ZLM 无需提前注册，收到即生成流
 *
 * 协议要求（JT/T 1078-2016 §5.5.4）：
 *  平台在收到设备视频数据期间，需按固定间隔发送 T9105（0x9105）状态通知，
 *  否则设备会停止推流。本类通过 SessionManager 找到设备的 JT808 会话并定期下发。
 */
@Slf4j
public class RTMPJTStreamHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final String         zlmHost;
    private final int            zlmRtmpPort;
    private final String         streamName;
    private final SessionManager sessionManager;

    private RtmpPusher                rtmpPusher;
    private final ByteArrayOutputStream frameBuffer = new ByteArrayOutputStream(65536);
    private long frameTimestamp = 0;

    // 从 JT1078 头部解析出的设备信息，用于发送 T9105
    private String clientId;   // SIM 卡号 → JT808 会话 key
    private int    channelNo;  // 逻辑通道号

    // T9105 定时任务：按标准每1秒发送一条，而不是每个子包都发
    private ScheduledFuture<?> t9105Task;
    private static final long T9105_INTERVAL_MS = 1000;

    public RTMPJTStreamHandler(String zlmHost, int zlmRtmpPort, String streamName, SessionManager sessionManager) {
        this.zlmHost        = zlmHost;
        this.zlmRtmpPort    = zlmRtmpPort;
        this.streamName     = streamName;
        this.sessionManager = sessionManager;
    }

    // ======================================================
    // 生命周期
    // ======================================================

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("JT1078设备连接 remote={}", ctx.channel().remoteAddress());
        rtmpPusher = new RtmpPusher(zlmHost, zlmRtmpPort, "live", streamName);
        try {
            rtmpPusher.connect();
        } catch (Exception e) {
            log.error("RTMP连接失败，本次视频将丢弃: {}", e.getMessage());
            rtmpPusher = null;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("JT1078设备断开 remote={}", ctx.channel().remoteAddress());

        // 停止 T9105 定时器
        if (t9105Task != null) {
            t9105Task.cancel(false);
            t9105Task = null;
        }

        // 若缓冲区还有数据（设备未发末包就断开），强制推出
        if (frameBuffer.size() > 0) {
            log.info("断开时缓冲 {}B 未推出，强制 flush", frameBuffer.size());
            flushFrame();
        }
        frameBuffer.reset();

        // 延迟 5 秒关闭 RTMP，给 ZLM 时间完成流索引，播放器可正常拉流
        ctx.channel().eventLoop().schedule(() -> {
            if (rtmpPusher != null) {
                rtmpPusher.close();
                rtmpPusher = null;
            }
        }, 5, TimeUnit.SECONDS);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException) {
            log.warn("JT1078连接异常 remote={} msg={}", ctx.channel().remoteAddress(), cause.getMessage());
        } else {
            log.error("JT1078推流异常 remote={}", ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }

    // ======================================================
    // 帧接收与缓冲
    // ======================================================

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        // LengthFieldBasedFrameDecoder 保证每次到达一帧完整数据（头30字节 + 数据体）
        if (msg.readableBytes() < 30) return;

        // ---- 解析 JT/T 1078 表19 固定头部（30字节）----
        msg.skipBytes(4);                // [0-3]  帧标识 0x30 0x31 0x63 0x64
        msg.skipBytes(1);                // [4]    V/P/X/CC
        msg.skipBytes(1);                // [5]    M/PT
        msg.skipBytes(2);                // [6-7]  JT1078 包序号
        byte[] simBytes = new byte[6];
        msg.readBytes(simBytes);         // [8-13] SIM 卡号（BCD，6字节）
        byte channel = msg.readByte();   // [14]   逻辑通道号
        byte subPkt  = msg.readByte();   // [15]   数据类型(高4bit) + 分包标记(低4bit)
        int  tsHigh  = msg.readInt();    // [16-19] PTS 高 32 位（ms）
        int  tsLow   = msg.readInt();    // [20-23] PTS 低 32 位（ms）
        msg.skipBytes(2);                // [24-25] Last I Frame Interval
        msg.skipBytes(2);                // [26-27] Last Frame Interval
        msg.skipBytes(2);                // [28-29] 数据体长度（帧解码器已用，此处跳过）
        // readerIndex == 30，后续全部是 H.264 Annex-B 数据体

        byte dataType = (byte) ((subPkt >> 4) & 0x0F); // 0=I帧 1=P帧 2=B帧 ≥3=音频/其他
        byte flag     = (byte) (subPkt & 0x0F);         // 0=原子包 1=首包 2=末包 3=中间包

        // 只处理视频帧（dataType 0/1/2）
        if (dataType > 2) {
            log.debug("非视频数据 dataType={} 跳过", dataType);
            return;
        }

        // 首次收包：提取 clientId 和通道号，启动 T9105 定时器
        if (clientId == null) {
            clientId  = parseSim(simBytes);
            channelNo = channel & 0xFF;
            log.info("JT1078首包 clientId={} channelNo={}", clientId, channelNo);
            startT9105Timer(ctx);
        }

        // 只在首包/原子包时更新时间戳（避免中间包覆盖）
        if (flag == 0 || flag == 1) {
            frameTimestamp = tsLow & 0xFFFFFFFFL;
        }

        int    payloadLen = msg.readableBytes();
        byte[] payload    = new byte[payloadLen];
        msg.readBytes(payload);
        frameBuffer.write(payload);

        log.info("子包 dataType={} flag={} payloadLen={}B 累计={}B ts={}ms",
                dataType, flag, payloadLen, frameBuffer.size(), frameTimestamp);

        // 异常 buffer 警戒：正常单帧不会超过 200KB，超过说明设备不发 flag=2 或分包异常
        if (frameBuffer.size() > 200 * 1024) {
            log.warn("帧缓冲异常过大 size={}B，可能设备未发 flag=2，强制丢弃避免内存爆炸", frameBuffer.size());
            frameBuffer.reset();
            return;
        }

        // 原子包(flag=0) 或 末包(flag=2)：一帧数据收齐，推流
        // 不再用 32KB 阈值强制 flush —— 某些设备单个 IDR 可能 > 60KB，提前 flush 会把关键帧切断导致花屏
        if (flag == 0 || flag == 2) {
            flushFrame();
        }
    }

    // ======================================================
    // T9105（通知设备继续推流）
    // ======================================================

    /**
     * 启动 T9105 定时器：首包后立即发第一条，之后每 1 秒一条
     * 按 JT/T 1078-2016 §5.5.4 要求"固定间隔发送"
     */
    private void startT9105Timer(ChannelHandlerContext ctx) {
        t9105Task = ctx.channel().eventLoop().scheduleAtFixedRate(
                this::sendT9105,
                0,                      // 立刻发第一条
                T9105_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        log.info("T9105定时器已启动 clientId={} channelNo={} interval={}ms",
                clientId, channelNo, T9105_INTERVAL_MS);
    }

    private void sendT9105() {
        log.info("T9105定时触发 clientId={} channelNo={}", clientId, channelNo);
        if (clientId == null || sessionManager == null) return;
        Session session = sessionManager.get(clientId);
        if (session == null || !session.isRegistered()) {
            log.warn("T9105: 未找到JT808会话，无法发送 clientId={}", clientId);
            return;
        }
        T9105 t9105 = new T9105();
        t9105.setMessageId(JT1078.实时音视频传输状态通知);
        t9105.setChannelNo(channelNo);
        t9105.setPacketLossRate(0);
        try {
            session.notify(t9105).block();
            log.info("T9105已发送 clientId={} channelNo={}", clientId, channelNo);
        } catch (Exception e) {
            log.warn("T9105发送失败 clientId={} err={}", clientId, e.getMessage());
        }
    }

    // ======================================================
    // 推流
    // ======================================================

    private void flushFrame() {
        byte[] annexBFrame = frameBuffer.toByteArray();
        frameBuffer.reset();

        if (annexBFrame.length == 0) return;

        if (rtmpPusher == null) {
            log.warn("RTMP未连接，丢弃帧 size={}B", annexBFrame.length);
            return;
        }

        log.info("组帧完成 size={}B ts={}ms，RTMP推流中...", annexBFrame.length, frameTimestamp);
        try {
            rtmpPusher.pushVideo(annexBFrame, frameTimestamp);
        } catch (Exception e) {
            log.error("RTMP推流失败: {}", e.getMessage(), e);
        }
    }

    // ======================================================
    // 工具
    // ======================================================

    /**
     * BCD 6字节 → 12位数字字符串（JT808 clientId 格式）
     * 例: 0x52 0x70 0x86 0x54 0x89 0x94 → "527086548994"
     */
    private static String parseSim(byte[] simBytes) {
        StringBuilder sb = new StringBuilder(12);
        for (byte b : simBytes) {
            sb.append((b >> 4) & 0x0F);
            sb.append(b & 0x0F);
        }
        return sb.toString();
    }
}
