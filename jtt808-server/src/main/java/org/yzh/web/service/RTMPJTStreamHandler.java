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
    /** 流名模板，支持 {client_id} / {channel_no} 占位符。等首个 JT1078 包到达才能解析出真实流名 */
    private final String         streamNameTemplate;
    private final SessionManager sessionManager;

    private RtmpPusher                rtmpPusher;
    private String                    resolvedStreamName; // 模板替换后的实际流名

    // 视频缓冲：单帧可能跨多个子包（flag=1 首，flag=3 中间，flag=2 尾）
    private final ByteArrayOutputStream videoBuffer = new ByteArrayOutputStream(65536);
    private long videoTimestamp = 0;

    // 音频缓冲：独立于视频，多数设备音频一包一帧（flag=0），也按分包逻辑兜底
    // 兼容模式：设备不发音频也完全 OK；发了支持编码就推，不支持的编码静默忽略（只记一次 WARN）
    private final ByteArrayOutputStream audioBuffer = new ByteArrayOutputStream(4096);
    private long    audioTimestamp   = 0;
    private int     audioPt          = -1;     // JT1078 PT 字段（6=G.711A, 7=G.711U, 19=AAC）
    private boolean audioCodecChecked = false; // 是否已判断过编码
    private boolean audioEnabled      = false; // 当前编码是否支持推流

    // 从 JT1078 头部解析出的设备信息，用于发送 T9105
    private String clientId;   // SIM 卡号 → JT808 会话 key
    private int    channelNo;  // 逻辑通道号

    // T9105 定时任务：按标准每1秒发送一条，而不是每个子包都发
    // 仅当有数据持续到达时才发送；超过 30s 无数据视为流已断开，停发 T9105 并关闭连接
    private ScheduledFuture<?> t9105Task;
    private ChannelHandlerContext handlerCtx; // 存储 ctx 引用，供 T9105 空闲超时关闭连接用
    private static final long T9105_INTERVAL_MS = 1000;
    private static final long T9105_IDLE_TIMEOUT_MS = 30_000;
    private long lastDataTime;             // 最近一次收到数据的时间戳（用于 T9105 空闲检测）

    public RTMPJTStreamHandler(String zlmHost, int zlmRtmpPort, String streamNameTemplate, SessionManager sessionManager) {
        this.zlmHost            = zlmHost;
        this.zlmRtmpPort        = zlmRtmpPort;
        this.streamNameTemplate = streamNameTemplate;
        this.sessionManager     = sessionManager;
    }

    // ======================================================
    // 生命周期
    // ======================================================

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handlerCtx = ctx;
        log.info("JT1078设备连接 remote={}，等待首包以确定流名（模板={}）",
                ctx.channel().remoteAddress(), streamNameTemplate);
        lastDataTime = System.currentTimeMillis();
        // RTMP 连接延迟到 channelRead0 中首包解析出 clientId/channelNo 后再建立
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("JT1078设备断开 remote={}", ctx.channel().remoteAddress());

        handlerCtx = null;

        // 停止 T9105 定时器
        if (t9105Task != null) {
            t9105Task.cancel(false);
            t9105Task = null;
        }

        // 若缓冲区还有数据（设备未发末包就断开），强制推出
        if (videoBuffer.size() > 0) {
            log.info("断开时视频缓冲 {}B 未推出，强制 flush", videoBuffer.size());
            flushVideoFrame();
        }
        if (audioBuffer.size() > 0) {
            log.info("断开时音频缓冲 {}B 未推出，强制 flush", audioBuffer.size());
            flushAudioFrame();
        }
        videoBuffer.reset();
        audioBuffer.reset();

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
        byte mpt     = msg.readByte();   // [5]    M(1) + PT(7) 编码类型
        int  pt      = mpt & 0x7F;
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
        // readerIndex == 30，后续全部是媒体数据体

        byte dataType = (byte) ((subPkt >> 4) & 0x0F); // 0=I帧 1=P帧 2=B帧 3=音频 ≥4=透传
        byte flag     = (byte) (subPkt & 0x0F);         // 0=原子包 1=首包 2=末包 3=中间包

        // 只处理视频(0/1/2) + 音频(3)
        if (dataType > 3) {
            log.debug("非音视频数据 dataType={} 跳过", dataType);
            return;
        }

        // 有数据到达 → 刷新 T9105 空闲计时器
        lastDataTime = System.currentTimeMillis();

        // 首次收包：提取 clientId 和通道号，解析流名模板并建立 RTMP 连接，启动 T9105 定时器
        if (clientId == null) {
            clientId  = parseSim(simBytes);
            channelNo = channel & 0xFF;
            log.info("JT1078首包 clientId={} channelNo={} pt={}", clientId, channelNo, pt);
            initRtmpPusher();
            startT9105Timer(ctx);
        }

        int    payloadLen = msg.readableBytes();
        byte[] payload    = new byte[payloadLen];
        msg.readBytes(payload);

        if (dataType <= 2) {
            handleVideo(dataType, flag, tsLow, payload);
        } else { // dataType == 3
            handleAudio(pt, flag, tsLow, payload);
        }
    }

    // ======================================================
    // 视频分支
    // ======================================================

    private void handleVideo(byte dataType, byte flag, int tsLow, byte[] payload) {
        if (flag == 0 || flag == 1) {
            videoTimestamp = tsLow & 0xFFFFFFFFL;
        }
        videoBuffer.write(payload, 0, payload.length);

        log.info("视频子包 dataType={} flag={} payloadLen={}B 累计={}B ts={}ms",
                dataType, flag, payload.length, videoBuffer.size(), videoTimestamp);

        if (videoBuffer.size() > 200 * 1024) {
            log.warn("视频缓冲异常过大 size={}B，强制丢弃避免内存爆炸", videoBuffer.size());
            videoBuffer.reset();
            return;
        }

        if (flag == 0 || flag == 2) {
            flushVideoFrame();
        }
    }

    // ======================================================
    // 音频分支
    // ======================================================

    private void handleAudio(int pt, byte flag, int tsLow, byte[] payload) {
        // 首包就确定编码是否支持，支持 → 正常推流；不支持 → 永久忽略，只记一次 WARN 不刷屏
        if (!audioCodecChecked) {
            audioCodecChecked = true;
            audioPt      = pt;
            audioEnabled = (pt == 6 || pt == 7 || pt == 19); // G.711A / G.711U / AAC
            if (audioEnabled) {
                log.info("音频启用 PT={} ({})", pt, codecName(pt));
            } else {
                log.warn("音频编码不支持 PT={} ({})，后续音频包将被静默丢弃，视频继续", pt, codecName(pt));
            }
        }
        if (!audioEnabled) return; // 兼容模式：不支持的编码直接跳过

        if (flag == 0 || flag == 1) {
            audioTimestamp = tsLow & 0xFFFFFFFFL;
        }
        audioBuffer.write(payload, 0, payload.length);

        log.info("音频子包 flag={} pt={} payloadLen={}B 累计={}B ts={}ms",
                flag, pt, payload.length, audioBuffer.size(), audioTimestamp);

        if (audioBuffer.size() > 64 * 1024) {
            log.warn("音频缓冲异常过大 size={}B，强制丢弃", audioBuffer.size());
            audioBuffer.reset();
            return;
        }

        if (flag == 0 || flag == 2) {
            flushAudioFrame();
        }
    }

    private static String codecName(int pt) {
        switch (pt) {
            case 6:  return "G.711A";
            case 7:  return "G.711U";
            case 8:  return "G.726";
            case 9:  return "G.729A";
            case 19: return "AAC(ADTS)";
            case 25: return "ADPCM";
            case 26: return "MP3";
            default: return "未知/不支持";
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
        long idle = System.currentTimeMillis() - lastDataTime;
        if (idle > T9105_IDLE_TIMEOUT_MS) {
            log.warn("T9105空闲超时 clientId={} channelNo={} idle={}ms，关闭JT1078连接",
                    clientId, channelNo, idle);
            if (t9105Task != null) {
                t9105Task.cancel(false);
                t9105Task = null;
            }
            if (handlerCtx != null) {
                handlerCtx.close();
            }
            return;
        }
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

    /**
     * 用已解析出的 clientId / channelNo 填充流名模板，并建立 RTMP 连接。
     * 支持占位符：{client_id}、{channel_no}
     */
    private void initRtmpPusher() {
        resolvedStreamName = streamNameTemplate
                .replace("{client_id}",  clientId)
                .replace("{channel_no}", String.valueOf(channelNo));
        log.info("解析流名 模板={} → 实际={}", streamNameTemplate, resolvedStreamName);

        rtmpPusher = new RtmpPusher(zlmHost, zlmRtmpPort, "live", resolvedStreamName);
        try {
            rtmpPusher.connect();
        } catch (Exception e) {
            log.error("RTMP连接失败，本次音视频将丢弃 stream={}: {}", resolvedStreamName, e.getMessage());
            rtmpPusher = null;
        }
    }

    private void flushVideoFrame() {
        byte[] annexBFrame = videoBuffer.toByteArray();
        videoBuffer.reset();

        if (annexBFrame.length == 0) return;

        if (rtmpPusher == null) {
            log.warn("RTMP未连接，丢弃视频帧 size={}B", annexBFrame.length);
            return;
        }

        log.info("视频组帧完成 size={}B ts={}ms，RTMP推流中...", annexBFrame.length, videoTimestamp);
        try {
            rtmpPusher.pushVideo(annexBFrame, videoTimestamp);
        } catch (Exception e) {
            log.error("RTMP视频推流失败: {}", e.getMessage(), e);
        }
    }

    private void flushAudioFrame() {
        byte[] audioFrame = audioBuffer.toByteArray();
        audioBuffer.reset();

        if (audioFrame.length == 0) return;

        if (rtmpPusher == null) {
            log.warn("RTMP未连接，丢弃音频帧 size={}B", audioFrame.length);
            return;
        }

        log.info("音频组帧完成 size={}B pt={} ts={}ms，RTMP推流中...", audioFrame.length, audioPt, audioTimestamp);
        try {
            rtmpPusher.pushAudio(audioFrame, audioPt, audioTimestamp);
        } catch (Exception e) {
            log.error("RTMP音频推流失败: {}", e.getMessage(), e);
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
