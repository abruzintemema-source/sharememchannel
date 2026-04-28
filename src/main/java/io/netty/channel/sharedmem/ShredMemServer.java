package io.netty.channel.sharedmem;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class SharedMemServer {

    private final int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public SharedMemServer(int port) {
        this.port = port;
    }
    
    public static void main(String[] args) throws Exception {

        SharedMemEventLoopGroup boss = new SharedMemEventLoopGroup(1);
        SharedMemEventLoopGroup worker = new SharedMemEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(SharedMemServerChannel.class)
                .childHandler(new SharedMemServerInitializer());

            // 🔥 IMPORTANT: this name MUST match client
            bootstrap.bind(new SharedMemAddress("test-service", 9000)).sync();

            System.out.println("🔥 SharedMem Server started");

            Thread.sleep(Long.MAX_VALUE);

        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }


    public void start() throws InterruptedException {

        bossGroup = new SharedMemEventLoopGroup(1);
        workerGroup = new SharedMemEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(SharedMemServerChannel.class)
            .childHandler(new SharedMemServerInitializer());

        bootstrap.bind(port).sync();

        System.out.println("SharedMemServer started on port " + port);
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
