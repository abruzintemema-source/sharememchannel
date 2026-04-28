package io.netty.channel.sharedmem;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.sharedmem.SharedMemChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class SharedMemServerInitializer extends ChannelInitializer<SharedMemChannel> {
    @Override
    protected void initChannel(SharedMemChannel ch) {
        ch.pipeline()
          .addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4))
          .addLast(new LengthFieldPrepender(4))
          .addLast(new SharedMemServerHandler());
    }
}
