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
    /** 服务级音频开关：false 时所有音频包静默丢弃。用于排查"音视频混合时 HLS 黑屏" */
    private final boolean        audioServerEnabled;
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
    private ScheduledFuture<?> t9105Task;
    private static final long T9105_INTERVAL_MS = 1000;

    // 心跳统计：每 30 秒打一条总览，方便排查"5 分钟自动断开"等长连接问题
    private ScheduledFuture<?> statsTask;
    private static final long STATS_INTERVAL_MS = 30_000;
    private long connectStartMs;
    private long videoFrameCount;
    private long audioFrameCount;
    private long lastVideoFlushMs;
    private long t9105Sent;
    private long t9105Failed;

    public RTMPJTStreamHandler(String zlmHost, int zlmRtmpPort, String streamNameTemplate,
                               boolean audioServerEnabled, SessionManager sessionManager) {
        this.zlmHost             = zlmHost;
        this.zlmRtmpPort         = zlmRtmpPort;
        this.streamNameTemplate  = streamNameTemplate;
        this.audioServerEnabled  = audioServerEnabled;
        this.sessionManager      = sessionManager;
    }

    // ======================================================
    // 生命周期
    // ======================================================

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        connectStartMs = System.currentTimeMillis();
        log.info("[JT1078连接] remote={}，等待首包以确定流名（模板={}）",
                ctx.channel().remoteAddress(), streamNameTemplate);
        // RTMP 连接延迟到 channelRead0 中首包解析出 clientId/channelNo 后再建立
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        long aliveSec = (System.currentTimeMillis() - connectStartMs) / 1000;
        log.info("[JT1078断开] remote={} clientId={} channelNo={} stream={} 存活={}s 视频帧={} 音频帧={} T9105发送={}/失败={}",
                ctx.channel().remoteAddress(), clientId, channelNo, resolvedStreamName,
                aliveSec, videoFrameCount, audioFrameCount, t9105Sent, t9105Failed);

        if (t9105Task != null) { t9105Task.cancel(false); t9105Task = null; }
        if (statsTask != null) { statsTask.cancel(false); statsTask = null; }

        // 若缓冲区还有数据（设备未发末包就断开），强制推出
        if (videoBuffer.size() > 0) flushVideoFrame();
        if (audioBuffer.size() > 0) flushAudioFrame();
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
        // JT1078FrameDecoder 已按 dataType 拆好整帧（视频 30B/音频 26B/透传 24B 头 + payload）
        // 这里必须按 dataType 走对应的 header 长度，不能固定 skip 30B
        if (msg.readableBytes() < 16) return;

        // ---- 共用前 16 字节（先读到 dataType 再决定剩余 header 怎么解析）----
        msg.skipBytes(4);                // [0-3]  帧标识 0x30 0x31 0x63 0x64
        msg.skipBytes(1);                // [4]    V/P/X/CC
        byte mpt     = msg.readByte();   // [5]    M(1) + PT(7) 编码类型
        int  pt      = mpt & 0x7F;
        msg.skipBytes(2);                // [6-7]  JT1078 包序号
        byte[] simBytes = new byte[6];
        msg.readBytes(simBytes);         // [8-13] SIM 卡号（BCD，6字节）
        byte channel = msg.readByte();   // [14]   逻辑通道号
        byte subPkt  = msg.readByte();   // [15]   数据类型(高4bit) + 分包标记(低4bit)

        byte dataType = (byte) ((subPkt >> 4) & 0x0F); // 0=I帧 1=P帧 2=B帧 3=音频 ≥4=透传
        byte flag     = (byte) (subPkt & 0x0F);         // 0=原子包 1=首包 2=末包 3=中间包

        // 只处理视频(0/1/2) + 音频(3)
        if (dataType > 3) {
            log.debug("非音视频数据 dataType={} 跳过", dataType);
            return;
        }

        // ---- 按 dataType 解析剩余 header ----
        // 视频(0/1/2): 16-19 tsHigh, 20-23 tsLow, 24-25 LastIFrame, 26-27 LastFrame, 28-29 dataLen
        // 音频(3):     16-19 tsHigh, 20-23 tsLow, 24-25 dataLen（无 LastIFrame/LastFrame）
        int tsLow;
        if (dataType <= 2) {
            msg.skipBytes(4);            // [16-19] PTS 高 32 位
            tsLow = msg.readInt();       // [20-23] PTS 低 32 位
            msg.skipBytes(6);            // [24-25] LastIFrame, [26-27] LastFrame, [28-29] dataLen
        } else {
            msg.skipBytes(4);            // [16-19] PTS 高 32 位
            tsLow = msg.readInt();       // [20-23] PTS 低 32 位
            msg.skipBytes(2);            // [24-25] dataLen（音频无 LastIFrame/LastFrame）
        }
        // readerIndex 现在指向媒体数据体起点

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
        // 诊断日志：每 30 帧打一次，确认设备 tsLow 是否在递增
        if (videoFrameCount % 30 == 0) {
            log.info("[视频诊断] frame#{} dataType={}({}) flag={}({}) tsLow={}ms videoTs(prev)={}ms payload={}B",
                    videoFrameCount,
                    dataType, dataType == 0 ? "I" : dataType == 1 ? "P" : "B",
                    flag, flag == 0 ? "原子" : flag == 1 ? "首" : flag == 2 ? "末" : "中",
                    tsLow & 0xFFFFFFFFL, videoTimestamp, payload.length);
        }
        if (flag == 0 || flag == 1) {
            videoTimestamp = tsLow & 0xFFFFFFFFL;
        }
        videoBuffer.write(payload, 0, payload.length);

        if (videoBuffer.size() > 200 * 1024) {
            log.warn("[视频异常] 缓冲过大 size={}B，强制丢弃", videoBuffer.size());
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
            boolean codecOk = (pt == 6 || pt == 7 || pt == 19); // G.711A / G.711U / AAC
            audioEnabled    = audioServerEnabled && codecOk;
            if (!audioServerEnabled) {
                log.info("音频已通过配置禁用 PT={} ({})，所有音频包将被静默丢弃", pt, codecName(pt));
            } else if (codecOk) {
                log.info("音频启用 PT={} ({})", pt, codecName(pt));
            } else {
                log.warn("音频编码不支持 PT={} ({})，后续音频包将被静默丢弃，视频继续", pt, codecName(pt));
            }
        }
        if (!audioEnabled) return; // 兼容模式：不支持或被禁用的编码直接跳过

        // 音频包不做分包重组：G.711A/G.711U/AAC 每个 1078 包就是一个完整可解码音频帧。
        // 早期按 flag==0/2 才 flush，但实际很多设备所有音频包 flag=1 或 3，导致缓冲累积到
        // 64KB 上限被丢（日志中大量 [音频异常] 缓冲过大），最终播放器一直收到 ZLM 补的静音。
        // 直接每包推流，时间戳就用本包自己的，最简单也最稳。
        audioTimestamp = tsLow & 0xFFFFFFFFL;
        audioBuffer.write(payload, 0, payload.length);
        flushAudioFrame();
    }

    private static String codecName(int pt) {
        switch (pt) {
            case 6:  return "G.711A";
            case 7:  return "G.711U";
            case 8:  return "G.726";
            case 9:  return "G.729A";
            case 19: return "AAC";
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
                this::sendT9105, 0, T9105_INTERVAL_MS, TimeUnit.MILLISECONDS);
        statsTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                this::printStats, STATS_INTERVAL_MS, STATS_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("[T9105启动] clientId={} channelNo={} interval={}ms",
                clientId, channelNo, T9105_INTERVAL_MS);
    }

    private void sendT9105() {
        if (clientId == null || sessionManager == null) return;
        Session session = sessionManager.get(clientId);
        if (session == null || !session.isRegistered()) {
            t9105Failed++;
            return; // 失败计数会在 30s 心跳和断开时汇总
        }
        T9105 t9105 = new T9105();
        t9105.setMessageId(JT1078.实时音视频传输状态通知);
        t9105.setChannelNo(channelNo);
        t9105.setPacketLossRate(0);
        try {
            session.notify(t9105).block();
            t9105Sent++;
        } catch (Exception e) {
            t9105Failed++;
        }
    }

    /** 30 秒一条总览，便于排查长连接问题（如设备每 5 分钟自动断开） */
    private void printStats() {
        long aliveSec = (System.currentTimeMillis() - connectStartMs) / 1000;
        long sinceLastVideo = lastVideoFlushMs > 0
                ? (System.currentTimeMillis() - lastVideoFlushMs) / 1000 : -1;
        log.info("[心跳] clientId={} 存活={}s 视频帧={} 音频帧={} 距上帧={}s T9105:{}/{}",
                clientId, aliveSec, videoFrameCount, audioFrameCount,
                sinceLastVideo, t9105Sent, t9105Failed);
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
        if (rtmpPusher == null) return; // 未连接静默丢弃，断开日志会汇总

        try {
            rtmpPusher.pushVideo(annexBFrame, videoTimestamp);
            videoFrameCount++;
            lastVideoFlushMs = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("[RTMP视频推流失败] err={}", e.getMessage());
        }
    }

    private void flushAudioFrame() {
        byte[] audioFrame = audioBuffer.toByteArray();
        audioBuffer.reset();
        if (audioFrame.length == 0) return;
        if (rtmpPusher == null) return;

        try {
            rtmpPusher.pushAudio(audioFrame, audioPt, audioTimestamp);
            audioFrameCount++;
        } catch (Exception e) {
            log.error("[RTMP音频推流失败] err={}", e.getMessage());
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
