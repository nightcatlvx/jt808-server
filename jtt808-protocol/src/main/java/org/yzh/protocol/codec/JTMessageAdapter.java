package org.yzh.protocol.codec;

import io.github.yezhihao.netmc.codec.MessageDecoder;
import io.github.yezhihao.netmc.codec.MessageEncoder;
import io.github.yezhihao.netmc.session.Session;
import io.github.yezhihao.protostar.SchemaManager;
import io.github.yezhihao.protostar.util.Explain;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.extern.slf4j.Slf4j;
import org.yzh.protocol.basics.JTMessage;

/**
 * JT消息编解码适配器
 * @author yezhihao
 * https://gitee.com/yezhihao/jt808-server
 */
@Slf4j
public class JTMessageAdapter implements MessageEncoder<JTMessage>, MessageDecoder<JTMessage> {

    private final JTMessageEncoder messageEncoder;

    private final JTMessageDecoder messageDecoder;

    public JTMessageAdapter(String... basePackages) {
        this(new SchemaManager(basePackages));
    }

    public JTMessageAdapter(SchemaManager schemaManager) {
        this(new JTMessageEncoder(schemaManager), new MultiPacketDecoder(schemaManager, new MultiPacketListener(20)));
    }

    public JTMessageAdapter(JTMessageEncoder messageEncoder, JTMessageDecoder messageDecoder) {
        this.messageEncoder = messageEncoder;
        this.messageDecoder = messageDecoder;
    }

    public ByteBuf encode(JTMessage message, Explain explain) {
        return messageEncoder.encode(message, explain);
    }

    public JTMessage decode(ByteBuf input, Explain explain) {
        return messageDecoder.decode(input, explain);
    }

    public ByteBuf encode(JTMessage message) {
        return messageEncoder.encode(message);
    }

    public JTMessage decode(ByteBuf input) {
        return messageDecoder.decode(input);
    }

    @Override
    public ByteBuf encode(JTMessage message, Session session) {
        ByteBuf output = messageEncoder.encode(message);
        encodeLog(session, message, output);
        return output;
    }

    @Override
    public JTMessage decode(ByteBuf input, Session session) {
        JTMessage message = messageDecoder.decode(input);
        if (message != null)
            message.setSession(session);
        decodeLog(session, message, input);
        return message;
    }

    public void encodeLog(Session session, JTMessage message, ByteBuf output) {
        if (log.isInfoEnabled())
            log.info("{}\n>>>>>-{},hex[{}]", session, message, formatHex(ByteBufUtil.hexDump(output)));
    }

    public void decodeLog(Session session, JTMessage message, ByteBuf input) {
        if (log.isInfoEnabled())
            log.info("{}\n<<<<<-{},hex[{}]", session, message, formatHex(ByteBufUtil.hexDump(input, 0, input.writerIndex())));
    }

    protected static String formatHex(String hex) {
        if (hex == null || hex.length() < 2) return hex;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hex.length(); i += 2) {
            if (i > 0) sb.append(' ');
            sb.append(hex, i, i + 2);
        }
        return sb.toString();
    }
}