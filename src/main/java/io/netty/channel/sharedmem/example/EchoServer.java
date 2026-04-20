package io.netty.channel.sharedmem.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.sharedmem.SharedMemAddress;
import io.netty.channel.sharedmem.SharedMemChannel;
import io.netty.channel.sharedmem.SharedMemChannelOption;
import io.netty.channel.sharedmem.SharedMemEventLoopGroup;
import io.netty.channel.sharedmem.SharedMemServerChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Example SharedMem echo server.
 *
 * Fix: SharedMemServerChannel now implements ServerChannel, so it is accepted
 *      by ServerBootstrap.channel(). Previously the bootstrap rejected it with
 *      "cannot be converted to Class<? extends ServerChannel>".
 */
public final class EchoServer {

    private static final int    REGION_SIZE = 8 * 1024 * 1024;
    private static final String REGION_NAME = "echo_server";

    public static void main(String[] args) throws InterruptedException {
        SharedMemEventLoopGroup bossGroup   = new SharedMemEventLoopGroup(1);
        SharedMemEventLoopGroup workerGroup = new SharedMemEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(SharedMemServerChannel.class)   // works now that ServerChannel is implemented
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childOption(SharedMemChannelOption.REGION_SIZE, REGION_SIZE)
                    .childOption(SharedMemChannelOption.MAX_MESSAGES_PER_READ, 64)
                    .childHandler(new ChannelInitializer<SharedMemChannel>() {
                        @Override
                        protected void initChannel(SharedMemChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(REGION_SIZE, 0, 4, 0, 4));
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new EchoServerHandler());
                        }
                    });

            ChannelFuture f = b.bind(new SharedMemAddress(REGION_NAME)).sync();
            System.out.println("[EchoServer] Listening on sharedmem://" + REGION_NAME);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static final class EchoServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            System.out.println("[EchoServer]  channelActive: " + ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ctx.writeAndFlush(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}