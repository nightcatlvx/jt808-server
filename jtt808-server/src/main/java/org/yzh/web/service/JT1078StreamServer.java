package org.yzh.web.service;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class JT1078StreamServer {

    private final int port;
    private final InetSocketAddress zlmAddress;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public JT1078StreamServer(int port, String zlmHost, int zlmRtpPort) {
        this.port = port;
        this.zlmAddress = new InetSocketAddress(zlmHost, zlmRtpPort);
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // 原始字节日志（在帧解码器之前，看设备发来的原始数据）
                                .addLast(new LoggingHandler("JT1078.RAW", LogLevel.INFO))
                                // JT/T 1078: 数据体长度字段在偏移28处，2字节WORD
                                .addLast(new LengthFieldBasedFrameDecoder(65535, 28, 2, 0, 0))
                                .addLast(new JT1078StreamHandler(zlmAddress));
                    }
                })
                .bind(port).sync()
                .addListener(f -> log.info("JT1078视频接收服务启动 port={} -> ZLM {}", port, zlmAddress));
    }

    public void stop() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("JT1078视频接收服务停止 port={}", port);
    }
}
