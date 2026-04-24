package org.yzh.web.service;

import io.github.yezhihao.netmc.session.SessionManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 * JT/T 1078 视频接收服务（RTMP 推流版）
 *
 * 与 JT1078StreamServer 的区别：
 *  - 旧版：收到视频帧后封装 RTP，TCP 推给 ZLM rtp_proxy 端口（需 openRtpServer 注册）
 *  - 新版：收到视频帧后推 RTMP，ZLM 自动识别流，无需提前注册
 *          播放地址: rtmp://<zlmHost>:<zlmRtmpPort>/live/<streamName>
 */
@Slf4j
public class RTMPJTStreamServer {

    private final int            port;
    private final String         zlmHost;
    private final int            zlmRtmpPort;
    private final String         streamName;
    private final SessionManager sessionManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    /**
     * @param port           本服务监听端口（设备连接此端口上传 JT1078 视频）
     * @param zlmHost        ZLMediaKit 地址
     * @param zlmRtmpPort    ZLMediaKit RTMP 端口（默认 1935）
     * @param streamName     RTMP 流名（播放 URL 中的 streamId）
     * @param sessionManager JT808 会话管理器（用于向设备发送 T9105 心跳）
     */
    public RTMPJTStreamServer(int port, String zlmHost, int zlmRtmpPort, String streamName, SessionManager sessionManager) {
        this.port           = port;
        this.zlmHost        = zlmHost;
        this.zlmRtmpPort    = zlmRtmpPort;
        this.streamName     = streamName;
        this.sessionManager = sessionManager;
    }

    public void start() throws InterruptedException {
        bossGroup  = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // JT/T 1078：数据体长度字段在偏移 28 处，2 字节，不含 30 字节头
                                // 完整帧 = 30字节头 + 数据体长度
                                .addLast(new LengthFieldBasedFrameDecoder(65535, 28, 2, 0, 0))
                                .addLast(new RTMPJTStreamHandler(zlmHost, zlmRtmpPort, streamName, sessionManager));
                    }
                })
                .bind(port).sync()
                .addListener(f -> log.info(
                        "RTMP-JT1078接收服务启动 port={} -> rtmp://{}:{}/live/{}",
                        port, zlmHost, zlmRtmpPort, streamName));
    }

    public void stop() {
        if (bossGroup  != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("RTMP-JT1078接收服务停止 port={}", port);
    }
}
