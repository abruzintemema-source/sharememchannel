package io.netty.channel.sharedmem;

import io.netty.channel.*;
import java.net.SocketAddress;
import java.util.Arrays;

public class SharedMemServerChannel extends AbstractSharedMemChannel implements ServerChannel {

    private static final ChannelMetadata SERVER_METADATA = new ChannelMetadata(false);

    public static final String CQ_SUFFIX = ".cq";
    private static final int CQ_CAPACITY = 512 * SharedMemConnectionToken.ENCODED_SIZE;

    private final SharedMemChannelConfig config;

    private volatile SharedMemRegion connectionQueueRegion;
    private volatile boolean readPending;

    public SharedMemServerChannel() {
        super(null);
        this.config = new SharedMemChannelConfig(this);
    }

    @Override
    public SharedMemChannelConfig config() {
        return config;
    }

    @Override
    public ChannelMetadata metadata() {
        return SERVER_METADATA;
    }

    @Override
    public boolean isOpen() {
        SharedMemRegion cq = connectionQueueRegion;
        return cq == null || !cq.isClosed();
    }

    @Override
    public boolean isActive() {
        SharedMemRegion cq = connectionQueueRegion;
        return active && cq != null && !cq.isClosed();
    }

    // ─────────────────────────────────────────────────────────────
    // BIND
    // ─────────────────────────────────────────────────────────────

    @Override
    protected void doBind(SocketAddress localAddr) throws Exception {
        System.out.println("🔥 SharedMemServerChannel.doBind()");

        java.net.InetSocketAddress addr = (java.net.InetSocketAddress) localAddr;
        String host = addr.getHostString();
        int port = addr.getPort();

        this.localAddress = addr;

        // String cqName = host + "_" + port + CQ_SUFFIX;
        String cqName = "localhost_9000.cq";

        System.out.println("🔥 CQ Region = " + cqName);

        this.connectionQueueRegion = new SharedMemRegion(
                cqName,
                CQ_CAPACITY,
                true
        );

        this.active = true;

        EventLoop loop = eventLoop();

        // ACCEPT LOOP
        loop.execute(new Runnable() {
            @Override
            public void run() {
                if (isActive()) {
                    pollAccept();
                    loop.execute(this);
                }
            }
        });
    }

    @Override
    protected void doDisconnect() throws Exception {
        doClose();
    }

    @Override
    protected void doConnect(SocketAddress remoteAddress, SocketAddress localAddress) {
        throw new UnsupportedOperationException("Server cannot connect");
    }

    @Override
    protected void doClose() throws Exception {
        active = false;

        SharedMemRegion cq = connectionQueueRegion;
        if (cq != null && !cq.isClosed()) {
            cq.close();
        }

        super.doClose();
    }

    @Override
    protected void doBeginRead() {
        readPending = true;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) {
        throw new UnsupportedOperationException("Server cannot write");
    }

    // ─────────────────────────────────────────────────────────────
    // ACCEPT LOOP
    // ─────────────────────────────────────────────────────────────

    public void pollAccept() {

        SharedMemRegion cq = this.connectionQueueRegion;

        if (cq == null || cq.isClosed()) {
            return;
        }

        int available = cq.readableBytes();

        // 🔴 IMPORTANT: do NOT log every loop iteration
        if (available < SharedMemConnectionToken.ENCODED_SIZE) {
            return;
        }

        ChannelPipeline pipeline = pipeline();
        boolean acceptedAny = false;

        try {

            while (cq.readableBytes() >= SharedMemConnectionToken.ENCODED_SIZE) {

                byte[] tokenBytes = new byte[SharedMemConnectionToken.ENCODED_SIZE];
                int read = cq.read(tokenBytes, 0, tokenBytes.length);

                if (read != SharedMemConnectionToken.ENCODED_SIZE) {
                    System.err.println("⚠️ Partial token read: " + read);
                    break;
                }

                System.out.println("🔥 RAW TOKEN: " + Arrays.toString(tokenBytes));

                SharedMemConnectionToken token;
                try {
                    token = SharedMemConnectionToken.decode(tokenBytes);
                } catch (Exception e) {
                    System.err.println("❌ Invalid token, skipping: " + e.getMessage());
                    continue;
                }

                System.out.println("🔥 ACCEPTING CONNECTION: " + token.getRegionName());

                SharedMemChannel child = createChildChannel(token);

                pipeline.fireChannelRead(child);

                acceptedAny = true;
            }

        } catch (Exception e) {
            System.err.println("❌ pollAccept error: " + e.getMessage());
            e.printStackTrace();

        } finally {
            if (acceptedAny) {
                pipeline.fireChannelReadComplete();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // CHILD CHANNEL CREATION
    // ─────────────────────────────────────────────────────────────

    private SharedMemChannel createChildChannel(SharedMemConnectionToken token) throws Exception {

        SharedMemChannel child = new SharedMemChannel(this);

        child.remoteAddress = new java.net.InetSocketAddress("client", token.getPort());
        child.localAddress = this.localAddress;

        // client → server
        child.rxRegion = new SharedMemRegion(
                token.getRegionName(),
                token.getRegionCapacity(),
                false
        );

        // server → client
        child.txRegion = new SharedMemRegion(
                token.getRegionName() + S2C_SUFFIX,
                token.getRegionCapacity(),
                true
        );
        String s2cName = token.getRegionName() + S2C_SUFFIX;
        child.active = true;

        System.out.println("🔥 Sending PING to client via S2C");

        // IMPORTANT: server → client channel
        byte[] ping = "PING".getBytes();

        int written = child.txRegion.write(ping, 0, ping.length);

        if (written != ping.length) {
            System.err.println("⚠️ Failed to write full PING. written=" + written);
        }
        System.out.println("🔥 S2C region = " + s2cName);
        System.out.println("🔥 S2C region name = " + s2cName);
        System.out.println("🔥 S2C capacity = " + child.txRegion.getCapacity());
        java.nio.file.Path p =
            io.netty.channel.sharedmem.SharedMemRegion.resolveBackingFile(s2cName);

        System.out.println("🔥 S2C file exists = " + java.nio.file.Files.exists(p));
        System.out.println("🔥 Created child channel: " + token.getRegionName());

        return child;
    }

    // ─────────────────────────────────────────────────────────────
    // UTIL
    // ─────────────────────────────────────────────────────────────

    static String cqRegionName(java.net.InetSocketAddress addr) {
        return addr.getHostString() + "_" + addr.getPort() + CQ_SUFFIX;
    }

    @Override
    protected SocketAddress localAddress0() {
        return localAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return null;
    }
    
    public boolean hasReadableData() {
    SharedMemRegion cq = connectionQueueRegion;
    return cq != null
            && !cq.isClosed()
            && cq.readableBytes() >= SharedMemConnectionToken.ENCODED_SIZE;
}
}
