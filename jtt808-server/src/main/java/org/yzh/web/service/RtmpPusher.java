package org.yzh.web.service;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * 极简 RTMP 推流客户端，向 ZLMediaKit 推送 H.264 视频
 * 流程: 握手 → connect → createStream → publish → onMetaData → AVC seq header → video data
 */
@Slf4j
public class RtmpPusher {

    private final String host;
    private final int port;
    private final String app;
    private final String streamName;

    private Socket socket;
    private DataInputStream in;
    private OutputStream out;

    /** 我们向服务器发送的 chunk 大小（需通过 Set Chunk Size 消息告知服务器） */
    private static final int TX_CHUNK_SIZE = 4096;
    /** 服务器默认 chunk 大小，收到 Set Chunk Size 后更新 */
    private int rxChunkSize = 128;

    // RTMP 消息类型
    private static final int MSG_SET_CHUNK_SIZE = 1;
    private static final int MSG_ACK            = 3;
    private static final int MSG_USER_CTRL      = 4;
    private static final int MSG_WIN_ACK_SIZE   = 5;
    private static final int MSG_SET_PEER_BW    = 6;
    private static final int MSG_AUDIO          = 8;
    private static final int MSG_VIDEO          = 9;
    private static final int MSG_DATA_AMF0      = 18; // 0x12
    private static final int MSG_CMD_AMF0       = 20; // 0x14

    private double invokeId  = 0;
    private int    msgStreamId = 0; // createStream 返回的 stream id

    // 每个 CSID 的 chunk 上下文，用于跨 chunk 拼装消息
    private final Map<Integer, ChunkCtx> chunkCtxMap = new HashMap<>();

    // H.264 参数集
    private byte[] sps;
    private byte[] pps;
    private boolean headerSent = false;
    private boolean spsPpsWarned = false;   // SPS/PPS 缺失只记一次 WARN

    // AAC 参数集：从首个 ADTS 帧解析得到 AudioSpecificConfig（ASC，2字节）
    // ZLM/播放器需要先收到 AAC sequence header（FLV: 0xAF 0x00 [ASC]）才能解码后续 raw AAC
    private byte[] aacAsc;
    private boolean aacSeqHeaderSent = false;

    // 时间戳归零基准：RTMP 时间戳从 0 开始单调递增。
    // 设备 JT1078 PTS 常为非常大的毫秒值（可能 > 2^31），直接用会导致：
    //  1) int 强转变负数；2) 超过 24bit（16.7s）后 chunk 头截断；3) 播放器花屏/卡住
    //
    // 音视频独立基准：设备可能为 A/V 使用不同 PTS 时钟（如视频用系统时间，音频用采样计数器），
    // 共享基准会导致一个轨道的时间戳被污染。ZLM modify_stamp=2 会按相对时间处理，独立归零安全。
    private long firstVideoTimestampMs = -1L;
    private long firstAudioTimestampMs = -1L;

    public RtmpPusher(String host, int port, String app, String streamName) {
        this.host       = host;
        this.port       = port;
        this.app        = app;
        this.streamName = streamName;
    }

    // ======================================================
    // 连接建立
    // ======================================================

    public void connect() throws Exception {
        log.info("RTMP连接 rtmp://{}:{}/{}/{}", host, port, app, streamName);
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(5000);
        in  = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new BufferedOutputStream(socket.getOutputStream());

        doHandshake();
        sendSetChunkSize(TX_CHUNK_SIZE); // 告知服务器我们的 chunk 大小
        sendConnect();
        doSetup();
        log.info("RTMP推流通道就绪 rtmp://{}:{}/{}/{}", host, port, app, streamName);
    }

    // ======================================================
    // RTMP 握手 (C0/C1/C2 ↔ S0/S1/S2)
    // ======================================================

    private void doHandshake() throws Exception {
        byte[] c1 = new byte[1536];
        new Random().nextBytes(c1);
        c1[0] = c1[1] = c1[2] = c1[3] = 0; // time = 0

        out.write(3);   // C0: version = 3
        out.write(c1);  // C1
        out.flush();

        int s0 = in.read();
        if (s0 != 3) throw new IOException("RTMP握手失败: S0=" + s0);

        byte[] s1 = new byte[1536];
        in.readFully(s1);
        byte[] s2 = new byte[1536];
        in.readFully(s2);

        out.write(s1);  // C2 = echo S1
        out.flush();
        log.debug("RTMP握手完成");
    }

    // ======================================================
    // 发送控制/命令消息
    // ======================================================

    private void sendSetChunkSize(int size) throws Exception {
        byte[] body = {(byte)(size>>24), (byte)(size>>16), (byte)(size>>8), (byte)size};
        sendChunk(2, 0, MSG_SET_CHUNK_SIZE, 0, body);
        out.flush();
    }

    private void sendConnect() throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        amfString(body, "connect");
        amfNumber(body, ++invokeId);
        body.write(0x03); // AMF object begin
        amfKV(body, "app",      app);
        amfKV(body, "type",     "nonprivate");
        amfKV(body, "flashVer", "FME/3.0 (compatible; FMSc/1.0)");
        amfKV(body, "tcUrl",    "rtmp://" + host + ":" + port + "/" + app);
        body.write(new byte[]{0x00, 0x00, 0x09}); // object end
        sendChunk(3, 0, MSG_CMD_AMF0, 0, body.toByteArray());
        out.flush();
    }

    private void sendCreateStream() throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        amfString(body, "createStream");
        amfNumber(body, ++invokeId);
        body.write(0x05); // null
        sendChunk(3, 0, MSG_CMD_AMF0, 0, body.toByteArray());
        out.flush();
    }

    private void sendPublish() throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        amfString(body, "publish");
        amfNumber(body, ++invokeId);
        body.write(0x05); // null
        amfString(body, streamName);
        amfString(body, "live");
        sendChunk(8, 0, MSG_CMD_AMF0, msgStreamId, body.toByteArray());
        out.flush();
    }

    private void sendMetaData() throws Exception {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        amfString(body, "@setDataFrame");
        amfString(body, "onMetaData");
        body.write(0x08);           // ECMA array
        // 必须声明 audiocodecid，否则 ZLM 把流当作"只有视频"。
        // 后到的音频包被 "add track too late" 拒收（ZLM MediaSink.cpp:37 警告）。
        writeInt32BE(body, 5);      // array count = 5 (videocodecid + 4 audio fields)
        amfKV(body, "videocodecid",    7.0);  // AVC/H.264
        amfKV(body, "audiocodecid",    7.0);  // 占位：告知 ZLM 有音频轨道
        amfKV(body, "audiosamplerate", 8000.0);
        amfKV(body, "audiosamplesize", 8.0);
        amfKV(body, "stereo",          0.0);  // mono
        body.write(new byte[]{0x00, 0x00, 0x09}); // object end
        sendChunk(4, 0, MSG_DATA_AMF0, msgStreamId, body.toByteArray());
        out.flush();
    }

    // ======================================================
    // 等待服务器响应，完成 connect → createStream → publish 握手
    // ======================================================

    private void doSetup() throws Exception {
        boolean connectOk = false;
        boolean streamOk  = false;
        long deadline = System.currentTimeMillis() + 8000;

        while (System.currentTimeMillis() < deadline) {
            RtmpMsg msg = readMsg();
            if (msg == null) continue;

            switch (msg.type) {
                case MSG_SET_CHUNK_SIZE:
                    rxChunkSize = (int) readInt32BEFromBytes(msg.data, 0);
                    log.debug("服务器chunk大小={}", rxChunkSize);
                    break;
                case MSG_WIN_ACK_SIZE:
                case MSG_SET_PEER_BW:
                case MSG_USER_CTRL:
                case MSG_ACK:
                    break; // 忽略控制消息
                case MSG_CMD_AMF0:
                    String cmd = amfReadString(msg.data, new int[1]);
                    log.debug("收到AMF命令: {}", cmd);
                    if ("_result".equals(cmd)) {
                        if (!connectOk) {
                            connectOk = true;
                            log.debug("connect _result，发送 createStream");
                            sendCreateStream();
                        } else if (!streamOk) {
                            streamOk   = true;
                            msgStreamId = parseCreateStreamResult(msg.data);
                            log.debug("createStream _result msgStreamId={}", msgStreamId);
                            sendPublish();
                        }
                    } else if ("onStatus".equals(cmd)) {
                        log.info("publish onStatus，推流通道就绪");
                        sendMetaData();
                        return; // setup 完成
                    } else if ("_error".equals(cmd)) {
                        throw new IOException("RTMP _error: " + new String(msg.data));
                    }
                    break;
                default:
                    break;
            }
        }
        throw new IOException("RTMP setup 超时");
    }

    private int parseCreateStreamResult(byte[] data) {
        int[] pos = {0};
        amfSkipValue(data, pos); // "_result"
        amfSkipValue(data, pos); // transaction id
        amfSkipValue(data, pos); // null
        // 下一个应该是 number（stream id）
        if (pos[0] < data.length && (data[pos[0]] & 0xFF) == 0x00) {
            pos[0]++;
            return (int) readDoubleBEFromBytes(data, pos[0]);
        }
        return 1; // 默认 stream id = 1
    }

    // ======================================================
    // 视频推流
    // ======================================================

    public synchronized void pushVideo(byte[] annexBFrame, long timestampMs) throws Exception {
        if (out == null) return;

        List<byte[]> nals = parseAnnexB(annexBFrame);
        if (nals.isEmpty()) {
            // 设备可能使用非标准封装（如 PT=98 厂商自定义格式、加密等），
            // 数据不含 Annex-B 起始码（00 00 00 01 / 00 00 01），parseAnnexB 无法切分。
            //
            // 策略：如果 AVC sequence header 已发送，将整帧当作一个 NAL 推出去，
            // 由解码器尝试容错（多数解码器对首位为 0xFD/0xBA 等非标准 NAL 会跳过）；
            // 如果 header 还没发，说明连 SPS/PPS 都没有，帧无能为力，只能丢弃。
            if (!headerSent) {
                String head = bytesToHex(annexBFrame, 0, Math.min(16, annexBFrame.length));
                log.warn("Annex-B无起始码且header未发送，丢弃帧 size={}B 前16B={}", annexBFrame.length, head);
                return;
            }
            log.info("非Annex-B数据，按单NAL推送 size={}B 前4B={}",
                    annexBFrame.length, bytesToHex(annexBFrame, 0, Math.min(4, annexBFrame.length)));
            nals = java.util.Collections.singletonList(annexBFrame);
        }

        // 视频独立时间戳基准
        if (firstVideoTimestampMs < 0) firstVideoTimestampMs = timestampMs;
        long relativeTs = timestampMs - firstVideoTimestampMs;
        if (relativeTs < 0) relativeTs = 0;
        if (relativeTs > 0xFFFFFFL) relativeTs = 0xFFFFFFL;

        // 提取 SPS / PPS
        for (byte[] nal : nals) {
            if (nal.length == 0) continue;
            int t = nal[0] & 0x1F;
            if (t == 7) { sps = nal; log.debug("SPS提取 len={}", nal.length); }
            else if (t == 8) { pps = nal; log.debug("PPS提取 len={}", nal.length); }
        }

        // 首次推流：先发 AVC sequence header（携带 SPS/PPS），时间戳用 0
        if (!headerSent) {
            if (sps == null || pps == null) {
                // 只记一次 WARN，不刷屏。当设备先发 P 帧再发 I 帧（常见于流刚建立时），
                // 所有 P 帧正常丢弃（没有 SPS/PPS 的 AVC bitstream 解码器无法解析）。
                // GOP 间隔内 I 帧自然到达后 header 就发出了。
                if (!spsPpsWarned) {
                    log.warn("缺少SPS/PPS，等待I帧到达后自动恢复(size={}B)", annexBFrame.length);
                    spsPpsWarned = true;
                }
                return;
            }
            sendAvcSequenceHeader(0);
            headerSent = true;
        }

        // 构建 AVCC 格式数据（4字节长度前缀 + NAL，跳过 SPS/PPS）
        boolean keyframe = false;
        ByteArrayOutputStream avcc = new ByteArrayOutputStream();
        for (byte[] nal : nals) {
            if (nal.length == 0) continue;
            int t = nal[0] & 0x1F;
            if (t == 7 || t == 8) continue; // SPS/PPS 已在 sequence header 中
            if (t == 5) keyframe = true;     // IDR = 关键帧
            writeInt32BE(avcc, nal.length);
            avcc.write(nal);
        }
        if (avcc.size() == 0) {
            log.debug("无视频 NAL 数据，跳过");
            return;
        }

        // FLV video tag body:
        //   [frameType(4bit) + codecId(4bit)] [avcPacketType(1)] [compositionTime(3)] [AVCC data]
        ByteArrayOutputStream video = new ByteArrayOutputStream();
        video.write(keyframe ? 0x17 : 0x27); // 1=keyframe/2=inter, 7=AVC
        video.write(0x01);                    // AVC NALU
        video.write(0x00); video.write(0x00); video.write(0x00); // composition time offset
        video.write(avcc.toByteArray());

        sendChunk(4, (int) relativeTs, MSG_VIDEO, msgStreamId, video.toByteArray());
        out.flush();
        log.info("RTMP视频帧 ts={}ms(raw={}) keyframe={} avcc={}B 源前16B={}",
                relativeTs, timestampMs, keyframe, avcc.size(),
                bytesToHex(annexBFrame, 0, Math.min(16, annexBFrame.length)));
    }

    // ======================================================
    // 音频推流（仅 G.711A/G.711U，其它编码暂不支持）
    //
    // FLV AudioTag 格式（首字节）：
    //   soundFormat(4) | soundRate(2) | soundSize(1) | soundType(1)
    //   soundFormat: 7=G.711A, 8=G.711U, 10=AAC
    // AAC: 必须先发 sequence header（[0xAF 0x00] + ASC 2B），再发 raw frame（[0xAF 0x01] + raw AAC）
    //      ASC 由首个 ADTS 帧头解析得到（profile / sample_rate_idx / channel_cfg）
    // ======================================================

    public synchronized void pushAudio(byte[] data, int jtPt, long timestampMs) throws Exception {
        if (out == null) return;
        if (data == null || data.length == 0) return;

        // 音频独立时间戳基准（设备 A/V PTS 可能来自不同时钟，共享基准会污染另一方）
        if (firstAudioTimestampMs < 0) firstAudioTimestampMs = timestampMs;
        long relativeTs = timestampMs - firstAudioTimestampMs;
        if (relativeTs < 0) relativeTs = 0;
        if (relativeTs > 0xFFFFFFL) relativeTs = 0xFFFFFFL;

        switch (jtPt) {
            case 6:  pushG711(data, (int) relativeTs, 0x70); return; // G.711A
            case 7:  pushG711(data, (int) relativeTs, 0x80); return; // G.711U
            case 19: pushAac  (data, (int) relativeTs);       return; // AAC(ADTS)
            default:
                log.warn("不支持的音频编码 PT={}，跳过（当前支持 G.711A/G.711U/AAC）", jtPt);
        }
    }

    /**
     * G.711 推流：数据按帧长切分，每帧独立时间戳。
     * G.711A/G.711U @8kHz 标准帧长 160B=20ms，也兼容 320B=40ms。
     * 设备可能一个 JT1078 包里塞多帧，若全打同一时间戳 → 播放器瞬间播完 → 脉冲。
     */
    private void pushG711(byte[] data, int ts, int flvHeader) throws Exception {
        // 帧长：优先 160B(20ms)，若 data 正好是 320B 的倍数则用 320B
        int frameLen = 160;
        int frameMs  = 20;
        if (data.length >= 320 && data.length % 320 == 0) {
            frameLen = 320;
            frameMs  = 40;
        }
        int offset = 0;
        int frameTs = ts;
        while (offset < data.length) {
            int len = Math.min(frameLen, data.length - offset);
            byte[] body = new byte[1 + len];
            body[0] = (byte) flvHeader;
            System.arraycopy(data, offset, body, 1, len);
            sendChunk(6, frameTs, MSG_AUDIO, msgStreamId, body);
            log.info("RTMP音频帧 ts={}ms g711 size={}B", frameTs, len);
            offset += len;
            frameTs += frameMs;
        }
        out.flush(); // 一次 flush，减少 syscall
    }

    /**
     * AAC 推流：JT/T 1078 通常以 ADTS 格式承载（每帧前 7B ADTS 头）。
     * 流程：
     *  1) 首帧解析 ADTS → 构造 ASC → 发 AAC sequence header（ts=0）
     *  2) 之后剥掉 ADTS 头，剩余 raw AAC 数据按 [0xAF 0x01] + payload 发送
     */
    private void pushAac(byte[] data, int ts) throws Exception {
        boolean isAdts = data.length >= 7
                && (data[0] & 0xFF) == 0xFF
                && (data[1] & 0xF0) == 0xF0;

        if (!isAdts) {
            if (!aacSeqHeaderSent) {
                log.debug("AAC raw 但尚未取到 ASC（缺 ADTS），丢弃 size={}", data.length);
                return;
            }
            sendAacRaw(data, ts);
            return;
        }

        // 一个 payload 里可能有多个连续 ADTS 帧，按 frame_length 切分
        int pos = 0;
        while (pos + 7 <= data.length) {
            if ((data[pos] & 0xFF) != 0xFF || (data[pos + 1] & 0xF0) != 0xF0) {
                log.debug("ADTS 帧间字节非 sync，剩余丢弃 pos={} total={}", pos, data.length);
                return;
            }
            int adtsHeaderLen = ((data[pos + 1] & 0x01) == 1) ? 7 : 9;
            int frameLength   = ((data[pos + 3] & 0x03) << 11)
                              | ((data[pos + 4] & 0xFF) <<  3)
                              | ((data[pos + 5] & 0xE0) >>  5);
            if (frameLength <= adtsHeaderLen || pos + frameLength > data.length) {
                log.debug("ADTS frame_length 异常 frameLength={} headerLen={} pos={} total={}",
                        frameLength, adtsHeaderLen, pos, data.length);
                return;
            }

            if (!aacSeqHeaderSent) {
                aacAsc = buildAscFromAdts(Arrays.copyOfRange(data, pos, pos + 7));
                if (aacAsc == null) return;
                sendAacSequenceHeader(0);
                aacSeqHeaderSent = true;
            }

            byte[] rawAac = Arrays.copyOfRange(data, pos + adtsHeaderLen, pos + frameLength);
            sendAacRaw(rawAac, ts);
            pos += frameLength;
        }
    }

    private void sendAacRaw(byte[] rawAac, int ts) throws Exception {
        byte[] body = new byte[2 + rawAac.length];
        body[0] = (byte) 0xAF; // soundFormat=10(AAC)
        body[1] = 0x01;        // AACPacketType: 1 = raw AAC frame
        System.arraycopy(rawAac, 0, body, 2, rawAac.length);
        sendChunk(6, ts, MSG_AUDIO, msgStreamId, body);
        out.flush();
        log.info("RTMP音频帧 ts={}ms aac-raw size={}B", ts, rawAac.length);
    }

    /**
     * 从 ADTS 头解析 AudioSpecificConfig（AAC-LC，通常 2 字节）
     *   ADTS byte2: profile(2) | sampling_freq_idx(4) | private(1) | channel_cfg_high(1)
     *   ADTS byte3: channel_cfg_low(2) | original(1) | home(1) | copyright_id_bit(1)
     *   ASC bits  : audioObjectType(5)=profile+1 | sampling_frequency_index(4) | channel_configuration(4) | 000(3)
     */
    private byte[] buildAscFromAdts(byte[] adts) {
        int profile     = ((adts[2] & 0xC0) >> 6);
        int sampleIdx   = ((adts[2] & 0x3C) >> 2);
        int channelCfg  = ((adts[2] & 0x01) << 2) | ((adts[3] & 0xC0) >> 6);
        int audioObjType = profile + 1;

        if (sampleIdx > 12 || channelCfg == 0 || channelCfg > 7) {
            log.warn("AAC ADTS 头异常 sampleIdx={} channelCfg={}", sampleIdx, channelCfg);
            return null;
        }
        int b0 = ((audioObjType & 0x1F) << 3) | ((sampleIdx & 0x0E) >> 1);
        int b1 = ((sampleIdx & 0x01) << 7) | ((channelCfg & 0x0F) << 3);
        log.info("AAC ASC 解析 profile={} sampleIdx={} channelCfg={} ASC={}{}",
                profile, sampleIdx, channelCfg,
                String.format("%02X", b0), String.format("%02X", b1));
        return new byte[]{(byte) b0, (byte) b1};
    }

    private void sendAacSequenceHeader(int timestamp) throws Exception {
        byte[] body = new byte[2 + aacAsc.length];
        body[0] = (byte) 0xAF; // AAC
        body[1] = 0x00;        // AACPacketType: 0 = sequence header
        System.arraycopy(aacAsc, 0, body, 2, aacAsc.length);
        sendChunk(6, timestamp, MSG_AUDIO, msgStreamId, body);
        out.flush();
        log.info("AAC sequence header发送完成 asc={}B ts={}", aacAsc.length, timestamp);
    }

    private void sendAvcSequenceHeader(int timestamp) throws Exception {
        // AVCDecoderConfigurationRecord (ISO 14496-15)
        ByteArrayOutputStream cfg = new ByteArrayOutputStream();
        cfg.write(1);          // configurationVersion
        cfg.write(sps[1]);     // AVCProfileIndication
        cfg.write(sps[2]);     // profile_compatibility
        cfg.write(sps[3]);     // AVCLevelIndication
        cfg.write(0xFF);       // reserved(6bit) + lengthSizeMinusOne=3
        cfg.write(0xE1);       // reserved(3bit) + numSPS=1
        writeInt16BE(cfg, sps.length);
        cfg.write(sps);
        cfg.write(1);          // numPPS
        writeInt16BE(cfg, pps.length);
        cfg.write(pps);

        ByteArrayOutputStream video = new ByteArrayOutputStream();
        video.write(0x17);     // keyframe + AVC
        video.write(0x00);     // AVC sequence header
        video.write(0x00); video.write(0x00); video.write(0x00); // composition time
        video.write(cfg.toByteArray());

        sendChunk(4, timestamp, MSG_VIDEO, msgStreamId, video.toByteArray());
        out.flush();
        log.info("AVC sequence header发送完成 sps={}B pps={}B ts={}", sps.length, pps.length, timestamp);
    }

    // ======================================================
    // RTMP Chunk 发送
    // ======================================================

    private void sendChunk(int csid, int timestamp, int msgType, int msgStreamId, byte[] data) throws Exception {
        int total  = data.length;
        int offset = 0;
        boolean first = true;
        while (offset < total) {
            int chunkLen = Math.min(TX_CHUNK_SIZE, total - offset);
            if (first) {
                // fmt=0: 完整消息头（11字节）
                out.write(csid & 0x3F);                 // basic header: fmt=0
                out.write((timestamp >> 16) & 0xFF);    // timestamp (3B)
                out.write((timestamp >>  8) & 0xFF);
                out.write( timestamp        & 0xFF);
                out.write((total >> 16) & 0xFF);        // message length (3B)
                out.write((total >>  8) & 0xFF);
                out.write( total        & 0xFF);
                out.write(msgType & 0xFF);              // message type (1B)
                out.write( msgStreamId        & 0xFF);  // stream id (4B, little-endian)
                out.write((msgStreamId >>  8) & 0xFF);
                out.write((msgStreamId >> 16) & 0xFF);
                out.write((msgStreamId >> 24) & 0xFF);
                first = false;
            } else {
                // fmt=3: 无头，续包
                out.write(0xC0 | (csid & 0x3F));
            }
            out.write(data, offset, chunkLen);
            offset += chunkLen;
        }
    }

    // ======================================================
    // RTMP 消息读取（setup 阶段）
    // ======================================================

    private RtmpMsg readMsg() throws Exception {
        // basic header
        int bh = in.read();
        if (bh < 0) return null;
        int fmt  = (bh >> 6) & 0x03;
        int csid = bh & 0x3F;
        if (csid == 0) {
            csid = in.read() + 64;
        } else if (csid == 1) {
            csid = in.read() + in.read() * 256 + 64;
        }

        ChunkCtx ctx = chunkCtxMap.computeIfAbsent(csid, k -> new ChunkCtx());

        // message header
        if (fmt == 0) {
            ctx.timestamp  = read3BE();
            ctx.msgLength  = read3BE();
            ctx.msgType    = in.read() & 0xFF;
            ctx.streamId   = readInt32LE();
            if (ctx.timestamp == 0xFFFFFF) ctx.timestamp = in.readInt(); // extended
        } else if (fmt == 1) {
            int delta      = read3BE();
            ctx.msgLength  = read3BE();
            ctx.msgType    = in.read() & 0xFF;
            if (delta == 0xFFFFFF) delta = in.readInt();
            ctx.timestamp += delta;
        } else if (fmt == 2) {
            int delta      = read3BE();
            if (delta == 0xFFFFFF) delta = in.readInt();
            ctx.timestamp += delta;
        }
        // fmt=3: 复用上一次 ctx，无需读头

        if (ctx.msgLength <= 0) return null;

        // 按 rxChunkSize 分块读取消息体
        byte[] data      = new byte[ctx.msgLength];
        int remaining    = ctx.msgLength;
        int off          = 0;
        while (remaining > 0) {
            int toRead = Math.min(rxChunkSize, remaining);
            in.readFully(data, off, toRead);
            off       += toRead;
            remaining -= toRead;
            if (remaining > 0) {
                in.read(); // 跳过续包 basic header（fmt=3，1字节）
            }
        }
        return new RtmpMsg(ctx.msgType, data);
    }

    // ======================================================
    // AMF0 编码
    // ======================================================

    private void amfString(OutputStream os, String s) throws Exception {
        byte[] b = s.getBytes("UTF-8");
        os.write(0x02);
        writeInt16BE(os, b.length);
        os.write(b);
    }

    private void amfNumber(OutputStream os, double v) throws Exception {
        os.write(0x00);
        long bits = Double.doubleToLongBits(v);
        for (int i = 7; i >= 0; i--) os.write((int)((bits >> (i * 8)) & 0xFF));
    }

    private void amfKV(OutputStream os, String key, String val) throws Exception {
        byte[] kb = key.getBytes("UTF-8");
        writeInt16BE(os, kb.length);
        os.write(kb);
        amfString(os, val);
    }

    private void amfKV(OutputStream os, String key, double val) throws Exception {
        byte[] kb = key.getBytes("UTF-8");
        writeInt16BE(os, kb.length);
        os.write(kb);
        amfNumber(os, val);
    }

    // ======================================================
    // AMF0 解码（仅 setup 阶段用，够用即可）
    // ======================================================

    private String amfReadString(byte[] data, int[] pos) {
        if (pos[0] >= data.length) return "";
        int type = data[pos[0]++] & 0xFF;
        if (type != 0x02) return "";
        if (pos[0] + 2 > data.length) return "";
        int len = ((data[pos[0]] & 0xFF) << 8) | (data[pos[0] + 1] & 0xFF);
        pos[0] += 2;
        if (pos[0] + len > data.length) return "";
        String s = new String(data, pos[0], len);
        pos[0] += len;
        return s;
    }

    private void amfSkipValue(byte[] data, int[] pos) {
        if (pos[0] >= data.length) return;
        int type = data[pos[0]++] & 0xFF;
        switch (type) {
            case 0x00: pos[0] += 8; break; // number
            case 0x01: pos[0] += 1; break; // boolean
            case 0x02: {                    // string
                if (pos[0] + 2 <= data.length) {
                    int len = ((data[pos[0]] & 0xFF) << 8) | (data[pos[0] + 1] & 0xFF);
                    pos[0] += 2 + len;
                }
                break;
            }
            case 0x05: break; // null
            default:   break;
        }
    }

    // ======================================================
    // Annex-B 解析
    // ======================================================

    private List<byte[]> parseAnnexB(byte[] data) {
        List<byte[]> nals = new ArrayList<>();
        int start = -1, i = 0;
        while (i < data.length) {
            int sc = startCodeLen(data, i);
            if (sc > 0) {
                if (start >= 0) nals.add(Arrays.copyOfRange(data, start, i));
                start = i + sc;
                i = start;
            } else {
                i++;
            }
        }
        if (start >= 0 && start < data.length) nals.add(Arrays.copyOfRange(data, start, data.length));
        return nals;
    }

    private int startCodeLen(byte[] data, int pos) {
        if (pos + 4 <= data.length && data[pos]==0 && data[pos+1]==0 && data[pos+2]==0 && data[pos+3]==1) return 4;
        if (pos + 3 <= data.length && data[pos]==0 && data[pos+1]==0 && data[pos+2]==1) return 3;
        return 0;
    }

    // ======================================================
    // IO 工具
    // ======================================================

    private int read3BE() throws Exception {
        return ((in.read() & 0xFF) << 16) | ((in.read() & 0xFF) << 8) | (in.read() & 0xFF);
    }

    private int readInt32LE() throws Exception {
        int b0 = in.read() & 0xFF, b1 = in.read() & 0xFF,
            b2 = in.read() & 0xFF, b3 = in.read() & 0xFF;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private static long readInt32BEFromBytes(byte[] d, int o) {
        return ((d[o] & 0xFFL) << 24) | ((d[o+1] & 0xFFL) << 16)
             | ((d[o+2] & 0xFFL) <<  8) |  (d[o+3] & 0xFFL);
    }

    private static double readDoubleBEFromBytes(byte[] d, int o) {
        long bits = 0;
        for (int i = 0; i < 8; i++) bits = (bits << 8) | (d[o + i] & 0xFFL);
        return Double.longBitsToDouble(bits);
    }

    private void writeInt32BE(OutputStream os, int v) throws Exception {
        os.write((v >> 24) & 0xFF); os.write((v >> 16) & 0xFF);
        os.write((v >>  8) & 0xFF); os.write( v        & 0xFF);
    }

    private void writeInt16BE(OutputStream os, int v) throws Exception {
        os.write((v >> 8) & 0xFF); os.write(v & 0xFF);
    }

    private static String bytesToHex(byte[] data, int offset, int len) {
        StringBuilder sb = new StringBuilder(len * 3);
        int end = Math.min(offset + len, data.length);
        for (int i = offset; i < end; i++) {
            if (i > offset) sb.append(' ');
            sb.append(String.format("%02X", data[i] & 0xFF));
        }
        return sb.toString();
    }

    // ======================================================
    // 关闭
    // ======================================================

    public synchronized void close() {
        try { if (out    != null) out.close();    } catch (Exception ignored) {}
        try { if (socket != null) socket.close(); } catch (Exception ignored) {}
        out    = null;
        socket = null;
        firstVideoTimestampMs = -1L;
        firstAudioTimestampMs = -1L;
        headerSent            = false;
        spsPpsWarned          = false;
        aacSeqHeaderSent      = false;
        aacAsc                = null;
        log.info("RTMP连接已关闭");
    }

    // ======================================================
    // 内部类
    // ======================================================

    private static class RtmpMsg {
        final int type;
        final byte[] data;
        RtmpMsg(int type, byte[] data) { this.type = type; this.data = data; }
    }

    private static class ChunkCtx {
        int timestamp, msgLength, msgType, streamId;
    }
}
