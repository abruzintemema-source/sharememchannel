package io.netty.channel.sharedmem.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.sharedmem.SharedMemChannel;
import io.netty.channel.sharedmem.SharedMemChannelOption;
import io.netty.channel.sharedmem.SharedMemEventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Example SharedMem echo client.
 *
 * <p>Connects to the localhost:9000, sends "Hello, SharedMem!" and prints
 * the echoed reply.
 *
 * Run with:
 * <pre>
 *   java -cp ... io.netty.channel.sharedmem.example.EchoClient
 * </pre>
 */
public final class EchoClient {

    private static final int    REGION_SIZE    = 8 * 1024 * 1024;
    private static final String HOST = "localhost";
    private static final int PORT = 9000;

    public static void main(String[] args) throws InterruptedException {
        SharedMemEventLoopGroup group = new SharedMemEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(SharedMemChannel.class)
             .option(SharedMemChannelOption.REGION_SIZE, REGION_SIZE)
             .option(SharedMemChannelOption.POLL_INTERVAL_US, 50)
             .handler(new ChannelInitializer<SharedMemChannel>() {
                 @Override
                 protected void initChannel(SharedMemChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast(new LengthFieldBasedFrameDecoder(
                             REGION_SIZE, 0, 4, 0, 4));
                     p.addLast(new LengthFieldPrepender(4));
                     p.addLast(new EchoClientHandler());
                 }
             });

            ChannelFuture f = b.connect(
                    new InetSocketAddress(HOST, PORT),
                    new InetSocketAddress(HOST, 0)
            ).sync();

            System.out.println("[EchoClient] Connected to " + HOST + ":" + PORT + " via SharedMemChannel");


            // Send a test message
            String message = "Hello, SharedMem!";
            System.out.println("[EchoClient] Sent:     " + message);
            f.channel().writeAndFlush(
                    Unpooled.copiedBuffer(message, StandardCharsets.UTF_8));

            // Wait a bit then close
            Thread.sleep(3000);
            f.channel().close().sync();

        } finally {
            group.shutdownGracefully();
        }
    }

    // -------------------------------------------------------------------------
    // Handler
    // -------------------------------------------------------------------------

    private static final class EchoClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
            try {
                String reply = buf.toString(StandardCharsets.UTF_8);
                System.out.println("[EchoClient] Received: " + reply);
            } finally {
                buf.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
