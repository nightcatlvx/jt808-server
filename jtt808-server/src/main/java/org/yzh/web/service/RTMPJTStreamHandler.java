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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    // SPS/PPS 备份：当超大 I 帧触发缓冲上限被丢弃时，先扫描并保存 SPS/PPS，
    // 后续 flushVideoFrame 时若帧缺少 Annex-B 起始码，自动补回，避免解码器永久黑屏
    private byte[] savedSps;
    private byte[] savedPps;

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
    private static final long T9105_INTERVAL_MS = 10_000;
    private static final long T9105_IDLE_TIMEOUT_MS = 30_000;
    private long lastDataTime;             // 最近一次收到数据的时间戳（用于 T9105 空闲检测）

    // RTMP 推流错误限流：RTMP 断开后设备 TCP 可能仍存活，每帧都会触发一次推流失败。
    // 为避免日志刷屏，同一类错误在限流窗口内只记一次 ERROR，其余降为 DEBUG。
    private static final long ERROR_THROTTLE_MS = 10_000;
    private String lastErrorKey;           // 上次 ERROR 日志的特征 key（类别+消息）
    private long   lastErrorTime;          // 上次 ERROR 日志的时间戳

    // 共享 RtmpPusher 池：设备会为同一通道开两条 JT1078 TCP 连接（一条传视频 PT=98，一条传音频 PT=6），
    // 但 ZLM 同一流名只能有一个发布者，第二条 publish 会被拒绝。
    // 此处按 streamName 复用同一个 RtmpPusher，引用计数归零才真正关闭。
    private static final ConcurrentHashMap<String, SharedPusher> pusherPool = new ConcurrentHashMap<>();

    // 回放标记：9201 与 9101 共用端口 27078，但 ZLM 不允许同一流名有两个发布者。
    // 9201 下发前通过 markPlayback() 标记 (clientId, channelNo)，
    // initRtmpPusher() 中通过 isPlaybackAndClear() 原子地消费该标记，
    // 若为回放则流名追加 _playback 后缀，与实时流区分。
    private static final ConcurrentHashMap<String, Boolean> PLAYBACK_MARKS = new ConcurrentHashMap<>();

    /** 标记某个设备+通道最近一次 27078 连接是回放（由 9201 REST 端点调用） */
    public static void markPlayback(String clientId, int channelNo) {
        String key = clientId + "_" + channelNo;
        PLAYBACK_MARKS.put(key, Boolean.TRUE);
        log.info("已标记回放流 key={}", key);
    }

    /** 检查并原子消费回放标记，返回 true 表示本次连接是回放 */
    private static boolean isPlaybackAndClear(String clientId, int channelNo) {
        String key = clientId + "_" + channelNo;
        return PLAYBACK_MARKS.remove(key) != null;
    }

    private static class SharedPusher {
        final RtmpPusher pusher;
        final AtomicInteger refCount = new AtomicInteger(1);
        SharedPusher(RtmpPusher pusher) { this.pusher = pusher; }
    }

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

        // 释放共享 Pusher 引用：计数归零立即关闭，不再等（设备会快速重连，等待反而导致旧流占坑）
        releasePusher();
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
        byte vpxcc   = msg.readByte();   // [4]    V/P/X/CC - V:version P:priority X:encrypt CC:reserved
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

        // RTMP 连接已断开 → 该 JT1078 连接已失去意义，直接关闭避免每帧触发推流异常
        if (rtmpPusher != null && rtmpPusher.isBroken()) {
            log.debug("RTMP已断，关闭JT1078连接 clientId={}", clientId);
            ctx.close();
            return;
        }

        // 首次收包：提取 clientId 和通道号，解析流名模板并建立 RTMP 连接，启动 T9105 定时器
        if (clientId == null) {
            clientId  = parseSim(simBytes);
            channelNo = channel & 0xFF;
            log.info("JT1078首包 clientId={} channelNo={} pt={} vpxcc=0x{} (V={} P={} X={} CC={})",
                    clientId, channelNo, pt,
                    String.format("%02X", vpxcc),
                    (vpxcc >> 7) & 1, (vpxcc >> 6) & 1, (vpxcc >> 5) & 1, vpxcc & 0x1F);
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

        log.debug("视频子包 dataType={} flag={} payloadLen={}B 累计={}B ts={}ms",
                dataType, flag, payload.length, videoBuffer.size(), videoTimestamp);

        if (videoBuffer.size() > 1024 * 1024) {
            log.warn("视频缓冲异常过大 size={}B，备份SPS/PPS后丢弃", videoBuffer.size());
            backupSpsPps();
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

        log.debug("音频子包 flag={} pt={} payloadLen={}B 累计={}B ts={}ms",
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
        // RTMP 已断时无需继续维持心跳，直接关闭（设备无流可推，重连才有意义）
        if (rtmpPusher != null && rtmpPusher.isBroken()) {
            log.warn("T9105: RTMP连接已断，停止心跳并关闭JT1078连接 clientId={}", clientId);
            if (t9105Task != null) {
                t9105Task.cancel(false);
                t9105Task = null;
            }
            if (handlerCtx != null) {
                handlerCtx.close();
            }
            return;
        }
        log.debug("T9105定时触发 clientId={} channelNo={}", clientId, channelNo);
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
            log.debug("T9105已发送 clientId={} channelNo={}", clientId, channelNo);
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
        boolean isPlayback = isPlaybackAndClear(clientId, channelNo);
        resolvedStreamName = streamNameTemplate
                .replace("{client_id}",  clientId)
                .replace("{channel_no}", String.valueOf(channelNo));
        if (isPlayback) {
            resolvedStreamName = resolvedStreamName + "_playback";
            log.info("回放模式，流名追加 _playback 后缀 clientId={} channelNo={} → {}", clientId, channelNo, resolvedStreamName);
        } else {
            log.info("解析流名 模板={} → 实际={}", streamNameTemplate, resolvedStreamName);
        }

        // 共享池：同一流名的第二条 JT1078 连接（如音频通道）复用已有 RTMP 连接，避免 ZLM 拒绝重复 publish
        SharedPusher sp = pusherPool.compute(resolvedStreamName, (k, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                log.info("复用已有RTMP连接 stream={} refCount={}", k, existing.refCount.get());
                return existing;
            }
            RtmpPusher p = new RtmpPusher(zlmHost, zlmRtmpPort, "live", resolvedStreamName);
            try {
                p.connect();
                log.info("新建RTMP连接 stream={}", k);
            } catch (Exception e) {
                log.error("RTMP连接失败 stream={}: {}", k, e.getMessage());
                return null;
            }
            return new SharedPusher(p);
        });
        rtmpPusher = sp != null ? sp.pusher : null;
    }

    /** 释放本连接对共享 Pusher 的引用，计数归零则立即关闭 RTMP */
    private void releasePusher() {
        if (resolvedStreamName == null) return;
        pusherPool.computeIfPresent(resolvedStreamName, (k, sp) -> {
            int n = sp.refCount.decrementAndGet();
            log.info("RTMP引用释放 stream={} refCount={}", k, n);
            if (n == 0) {
                sp.pusher.close();
                log.info("RTMP共享连接已关闭 stream={}", k);
                return null;
            }
            return sp;
        });
        rtmpPusher = null;
    }

    private void flushVideoFrame() {
        byte[] frame = videoBuffer.toByteArray();
        videoBuffer.reset();

        if (frame.length == 0) return;

        if (rtmpPusher == null) {
            log.warn("RTMP未连接，丢弃视频帧 size={}B", frame.length);
            return;
        }

        // 如果之前因缓冲溢出备份了 SPS/PPS，且当前帧缺少 Annex-B 起始码，
        // 则将 SPS/PPS 以 Annex-B 格式补回帧头，让 RtmpPusher 能正确发送 AVC sequence header
        if (savedSps != null && savedPps != null && !hasAnnexBStartCode(frame)) {
            log.info("补回SPS/PPS到帧头 spsLen={} ppsLen={} frameLen={}", savedSps.length, savedPps.length, frame.length);
            ByteArrayOutputStream fixed = new ByteArrayOutputStream(frame.length + savedSps.length + savedPps.length + 8);
            try {
                fixed.write(new byte[]{0, 0, 0, 1});
                fixed.write(savedSps);
                fixed.write(new byte[]{0, 0, 0, 1});
                fixed.write(savedPps);
                fixed.write(frame);
            } catch (Exception ignored) { }
            frame = fixed.toByteArray();
            // 用完即清，避免后续帧重复补回
            savedSps = null;
            savedPps = null;
        }

        log.debug("视频组帧完成 size={}B ts={}ms，RTMP推流中...", frame.length, videoTimestamp);
        try {
            rtmpPusher.pushVideo(frame, videoTimestamp);
        } catch (Exception e) {
            logRtmpError("视频", e);
            // RTMP 连接已确认断开 → 关闭 JT1078 通道，阻止后续帧继续触发异常刷屏
            if (rtmpPusher.isBroken() && handlerCtx != null) {
                log.warn("RTMP连接已断，主动关闭JT1078连接 clientId={}", clientId);
                handlerCtx.close();
            }
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

        log.debug("音频组帧完成 size={}B pt={} ts={}ms，RTMP推流中...", audioFrame.length, audioPt, audioTimestamp);
        try {
            rtmpPusher.pushAudio(audioFrame, audioPt, audioTimestamp);
        } catch (Exception e) {
            logRtmpError("音频", e);
            // RTMP 连接已确认断开 → 关闭 JT1078 通道
            if (rtmpPusher.isBroken() && handlerCtx != null) {
                log.warn("RTMP连接已断，主动关闭JT1078连接 clientId={}", clientId);
                handlerCtx.close();
            }
        }
    }

    // ======================================================
    // 工具
    // ======================================================

    /**
     * 扫描 videoBuffer 中的 Annex-B 起始码，提取 SPS (NAL type=7) 和 PPS (NAL type=8) 并保存。
     * 仅在缓冲溢出时调用，防止因丢弃过大的 I 帧而永久丢失编解码参数。
     */
    private void backupSpsPps() {
        byte[] data = videoBuffer.toByteArray();
        int len = data.length;
        int i = 0;
        while (i < len - 4) {
            // 匹配 Annex-B 起始码: 00 00 00 01 或 00 00 01
            int nalStart;
            if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 0 && data[i + 3] == 1) {
                nalStart = i + 4;
                i += 4;
            } else if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1) {
                nalStart = i + 3;
                i += 3;
            } else {
                i++;
                continue;
            }
            if (nalStart >= len) break;
            int nalType = data[nalStart] & 0x1F;
            // 找到下一个起始码的位置作为 NAL 结束
            int end = len;
            for (int j = i; j < len - 3; j++) {
                if ((data[j] == 0 && data[j + 1] == 0 && data[j + 2] == 0 && data[j + 3] == 1)
                        || (data[j] == 0 && data[j + 1] == 0 && data[j + 2] == 1)) {
                    end = j;
                    break;
                }
            }
            int nalLen = end - nalStart;
            if (nalType == 7 && savedSps == null) {
                savedSps = new byte[nalLen];
                System.arraycopy(data, nalStart, savedSps, 0, nalLen);
                log.info("溢出前备份SPS len={}", nalLen);
            } else if (nalType == 8 && savedPps == null) {
                savedPps = new byte[nalLen];
                System.arraycopy(data, nalStart, savedPps, 0, nalLen);
                log.info("溢出前备份PPS len={}", nalLen);
            }
            i = end;
        }
    }

    /**
     * RTMP 推流错误限流日志。
     * 同一错误在 ERROR_THROTTLE_MS 窗口内只打一次 ERROR（含堆栈），
     * 之后的降级为 DEBUG（仅消息），避免刷屏。
     */
    private void logRtmpError(String cat, Exception e) {
        String key = cat + ":" + e.getClass().getSimpleName() + ":" + e.getMessage();
        long now = System.currentTimeMillis();
        if (key.equals(lastErrorKey) && (now - lastErrorTime) < ERROR_THROTTLE_MS) {
            log.debug("RTMP{}推流失败(限流): {}", cat, e.getMessage());
            return;
        }
        lastErrorKey  = key;
        lastErrorTime = now;
        log.error("RTMP{}推流失败: {}", cat, e.getMessage(), e);
    }

    /** 检查数据是否以 Annex-B 起始码（00 00 00 01 或 00 00 01）开头 */
    private static boolean hasAnnexBStartCode(byte[] data) {
        if (data.length < 4) return false;
        return (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 1)
                || (data[0] == 0 && data[1] == 0 && data[2] == 1);
    }

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
