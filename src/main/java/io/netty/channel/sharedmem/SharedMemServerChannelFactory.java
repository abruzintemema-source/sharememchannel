package io.netty.channel.sharedmem;

import io.netty.channel.*;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class SharedMemServerChannel extends AbstractSharedMemChannel implements ServerChannel {

    private static final ChannelMetadata SERVER_METADATA = new ChannelMetadata(false);

    private static final InternalLogger LOG =
            InternalLoggerFactory.getInstance(SharedMemServerChannel.class);

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
        if (!(localAddr instanceof InetSocketAddress)) {
            throw new IllegalArgumentException(
                    "SharedMemServerChannel requires localAddress to be InetSocketAddress, got: "
                            + (localAddr == null ? "null" : localAddr.getClass().getName()));
        }

        InetSocketAddress addr = (InetSocketAddress) localAddr;
        this.localAddress = addr;

        String cqName = SharedMemEndpointKey.cqRegionName(addr);
        if (LOG.isInfoEnabled()) {
            LOG.info("Binding sharedmem server local={} cqRegion={}", localAddr, cqName);
        }
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
                    LOG.warn("Partial connection token read: {}", read);
                    break;
                }

                SharedMemConnectionToken token;
                try {
                    token = SharedMemConnectionToken.decode(tokenBytes);
                } catch (Exception e) {
                    LOG.warn("Invalid connection token, skipping", e);
                    continue;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("Accepting sharedmem connection region={} clientPort={}",
                            token.getRegionName(), token.getPort());
                }


                SharedMemChannel child = createChildChannel(token);

                pipeline.fireChannelRead(child);

                acceptedAny = true;
            }

        } catch (Exception e) {
            LOG.error("pollAccept failure", e);

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
        child.active = true;
        // IMPORTANT: server → client channel
        byte[] ping = "PING".getBytes();

        int written = child.txRegion.write(ping, 0, ping.length);

        return child;
    }

    // ─────────────────────────────────────────────────────────────
    // UTIL
    // ─────────────────────────────────────────────────────────────

    // static String cqRegionName(InetSocketAddress addr) {
    //     return addr.getHostString() + "_" + addr.getPort() + CQ_SUFFIX;
    // }

    // static String cqRegionName(SocketAddress addr) {
    //     if (addr instanceof SharedMemAddress) {
    //         return ((SharedMemAddress) addr).getRegionName() + CQ_SUFFIX;
    //     }
    //     if (addr instanceof InetSocketAddress) {
    //         return cqRegionName((InetSocketAddress) addr);
    //     }
    //     throw new IllegalArgumentException(
    //             "Unsupported address type for CQ naming: " +
    //                     (addr == null ? "null" : addr.getClass().getName()));
    // }

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
