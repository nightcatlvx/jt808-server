package org.yzh.web.service;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * JT/T 1078 帧解码器
 *
 * 不能用 {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder} 一刀切，因为
 * JT/T 1078 表19 三种包头长度不同，长度字段位置也不同：
 *
 * | 包类型              | dataType (offset 15 高4位) | header 长度 | 长度字段位置 |
 * |---------------------|----------------------------|-------------|--------------|
 * | I/P/B 视频          | 0 / 1 / 2                  | 30 字节     | offset 28-29 |
 * | 音频                | 3                          | 26 字节     | offset 24-25 |
 * | 透传                | ≥ 4                        | 24 字节     | offset 22-23 |
 *
 * 音频包没有 LastIFrameInterval(2B) 和 LastFrameInterval(2B) 两个字段。
 * 透传包再去掉 PTS 高低 32 位（共 8B）。
 *
 * 用统一的 LengthFieldBasedFrameDecoder(offset=28) 时，音频/透传包会从错误的位置读长度
 * → 帧切错位 → 视频 NALU 拼接被污染 → 绿屏 + 解析雪崩。
 *
 * 这个 decoder 先看 dataType 决定 header 长度和长度字段位置，再切 frame。
 */
@Slf4j
public class JT1078FrameDecoder extends ByteToMessageDecoder {

    /** 帧标识 0x30 0x31 0x63 0x64 */
    private static final int FRAME_HEADER_MAGIC = 0x30316364;
    /** 单帧最大长度防御上限（数据体），避免被恶意/错位长度撑爆内存 */
    private static final int MAX_PAYLOAD_LEN    = 65535;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (true) {
            // 至少要能看到 dataType 字段（在 offset 15）
            if (in.readableBytes() < 16) return;

            // 检查帧标识，错位时丢字节重新对齐
            int magic = in.getInt(in.readerIndex());
            if (magic != FRAME_HEADER_MAGIC) {
                int dropAt = findNextMagic(in);
                if (dropAt < 0) {
                    // 没找到，丢到只剩 4 字节（保留可能的 magic 起始）
                    int drop = Math.max(0, in.readableBytes() - 4);
                    if (drop > 0) {
                        log.warn("[1078帧] 帧标识错乱，丢 {} 字节重新对齐", drop);
                        in.skipBytes(drop);
                    }
                    return;
                }
                int drop = dropAt - in.readerIndex();
                log.warn("[1078帧] 帧标识错乱，丢 {} 字节后重新对齐", drop);
                in.skipBytes(drop);
                continue;
            }

            // dataType 高 4 位（offset 15）
            byte subPkt   = in.getByte(in.readerIndex() + 15);
            int  dataType = (subPkt >> 4) & 0x0F;

            int headerLen;       // 包头长度
            int lenFieldOffset;  // 数据体长度字段在包头里的偏移
            if (dataType <= 2) {
                headerLen      = 30;
                lenFieldOffset = 28;
            } else if (dataType == 3) {
                headerLen      = 26;
                lenFieldOffset = 24;
            } else {
                // 透传包，header 24 字节，长度字段 22-23
                headerLen      = 24;
                lenFieldOffset = 22;
            }

            if (in.readableBytes() < headerLen) return;

            int payloadLen = in.getUnsignedShort(in.readerIndex() + lenFieldOffset);
            if (payloadLen > MAX_PAYLOAD_LEN) {
                // 异常长度：丢一字节重新搜索 magic
                log.warn("[1078帧] dataType={} 异常长度 {}B，丢字节重对齐", dataType, payloadLen);
                in.skipBytes(1);
                continue;
            }

            int totalLen = headerLen + payloadLen;
            if (in.readableBytes() < totalLen) return;

            // 切出整帧（含完整头）传给上层 handler，让其按 dataType 自行解析
            ByteBuf frame = in.readRetainedSlice(totalLen);
            out.add(frame);
        }
    }

    /** 在 buf 中搜索下一个帧标识起点，返回绝对 readerIndex 位置；找不到返回 -1。 */
    private static int findNextMagic(ByteBuf in) {
        int start = in.readerIndex() + 1; // 跳过当前 1 字节
        int end   = in.writerIndex() - 4;
        for (int i = start; i <= end; i++) {
            if (in.getInt(i) == FRAME_HEADER_MAGIC) return i;
        }
        return -1;
    }
}
