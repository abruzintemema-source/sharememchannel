package io.netty.channel.sharedmem.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    private static final String HOST = "localhost";
    private static final int PORT = 9000;

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

            ChannelFuture f = b.bind(new InetSocketAddress(HOST, PORT)).sync();
            System.out.println("[EchoServer] Listening on " + HOST + ":" + PORT + " via SharedMemServerChannel");
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static final class EchoServerHandler extends ChannelInboundHandlerAdapter {

        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            System.out.println("[EchoServer]  channelActive: " + ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf in = (ByteBuf) msg;
            try {
                String received = in.toString(StandardCharsets.UTF_8);
                String timestamp = LocalDateTime.now().format(FMT);
                System.out.println("[EchoServer]  Received: " + received);
                System.out.println("[EchoServer]  Echoing at: " + timestamp);
                String reply = received + " over " + timestamp;
                ctx.writeAndFlush(Unpooled.copiedBuffer(reply, StandardCharsets.UTF_8));
            } finally {
                in.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
