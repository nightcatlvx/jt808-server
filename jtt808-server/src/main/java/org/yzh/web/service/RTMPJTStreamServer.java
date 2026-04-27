package org.yzh.web.service;

import io.github.yezhihao.netmc.session.SessionManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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
    /** 流名模板，支持 {client_id} / {channel_no} 占位符 */
    private final String         streamNameTemplate;
    /** 服务级音频开关，false 时所有连接均不推音频 */
    private final boolean        audioEnabled;
    private final SessionManager sessionManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    /**
     * @param port               本服务监听端口（设备连接此端口上传 JT1078 视频）
     * @param zlmHost            ZLMediaKit 地址
     * @param zlmRtmpPort        ZLMediaKit RTMP 端口（默认 1935）
     * @param streamNameTemplate RTMP 流名模板，支持 {client_id} / {channel_no} 占位符
     *                           例：{client_id}/{channel_no} 会解析为如 "101260130082/1"
     * @param audioEnabled       是否推送音频（false 时所有音频包静默丢弃，仅推视频）
     * @param sessionManager     JT808 会话管理器（用于向设备发送 T9105 心跳）
     */
    public RTMPJTStreamServer(int port, String zlmHost, int zlmRtmpPort, String streamNameTemplate,
                              boolean audioEnabled, SessionManager sessionManager) {
        this.port               = port;
        this.zlmHost            = zlmHost;
        this.zlmRtmpPort        = zlmRtmpPort;
        this.streamNameTemplate = streamNameTemplate;
        this.audioEnabled       = audioEnabled;
        this.sessionManager     = sessionManager;
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
                                // JT/T 1078 三种包头长度不同（视频30/音频26/透传24），
                                // 长度字段位置也不同。必须用自定义 decoder 区分 dataType。
                                .addLast(new JT1078FrameDecoder())
                                .addLast(new RTMPJTStreamHandler(zlmHost, zlmRtmpPort, streamNameTemplate, audioEnabled, sessionManager));
                    }
                })
                .bind(port).sync()
                .addListener(f -> log.info(
                        "RTMP-JT1078接收服务启动 port={} -> rtmp://{}:{}/live/{}（模板，按设备首包动态解析）",
                        port, zlmHost, zlmRtmpPort, streamNameTemplate));
    }

    public void stop() {
        if (bossGroup  != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("RTMP-JT1078接收服务停止 port={}", port);
    }
}
