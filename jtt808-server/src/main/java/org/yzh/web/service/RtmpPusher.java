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

    // 时间戳归零基准：RTMP 时间戳应从 0 开始单调递增。
    // 设备 JT1078 PTS 常为非常大的毫秒值（可能 > 2^31），直接用会导致：
    //  1) int 强转变负数；2) 超过 24bit（16.7s）后 chunk 头截断；3) 播放器花屏/卡住
    private long firstTimestampMs = -1L;

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
        writeInt32BE(body, 1);      // array count
        amfKV(body, "videocodecid", 7.0);
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
            // 正常情况下不会走到这里：设备应发标准 Annex-B，首包(flag=1)带 00 00 00 01
            // 走到这里 = 帧被截断（中间片段丢了首段）或设备发了非 Annex-B 格式
            // 此时硬推出去只会让解码器崩花屏，直接丢弃 + hex 打印便于排查
            String head = bytesToHex(annexBFrame, 0, Math.min(16, annexBFrame.length));
            log.warn("Annex-B无起始码，丢弃帧 size={}B 前16B={}", annexBFrame.length, head);
            return;
        }

        // 时间戳归零：首帧作为基准，后续全部减去它，这样 AVC header = 0，之后单调递增的小整数
        if (firstTimestampMs < 0) firstTimestampMs = timestampMs;
        long relativeTs = timestampMs - firstTimestampMs;
        if (relativeTs < 0) relativeTs = 0;                 // 防御：设备 PTS 回退
        // 24bit 上限 ≈ 4.66 小时，短流完全够用；超限硬停在上限避免 chunk header 截断伪造时间
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
                log.warn("缺少SPS/PPS，等待下一帧");
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
        firstTimestampMs = -1L;
        headerSent       = false;
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
