package io.netty.channel.sharedmem;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class SharedMemServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        System.out.println("📥 Server received: " + msg);

        // 🔥 echo back
        ctx.writeAndFlush(msg);
    }
}
