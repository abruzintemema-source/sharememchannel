package io.netty.channel.sharedmem;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.util.UUID;

/**
 * A Netty {@link Channel} that communicates over a dedicated shared-memory
 * ring buffer using the SharedMem protocol.
 *
 * <h3>Connection lifecycle (client side)</h3>
 * <ol>
 *   <li>{@link #doConnect} generates a <b>unique data region name</b> for this
 *       connection (no two connections share a region).</li>
 *   <li>Creates the data region backing file with {@code create=true}
 *       (truncates any stale file with the same name).</li>
 *   <li>Opens the server's CQ region ({@code serverRegionName + ".cq"}) and
 *       writes a {@link SharedMemConnectionToken} containing the data-region
 *       name and this client's port.</li>
 *   <li>The server's boss loop calls {@link SharedMemServerChannel#pollAccept()},
 *       reads the token, opens the same data region from the server side, and
 *       produces an accepted child channel — <b>without creating any new file</b>.</li>
 * </ol>
 *
 * <h3>Bootstrap example</h3>
 * <pre>{@code
 * EventLoopGroup group = new SharedMemEventLoopGroup(1);
 * Bootstrap b = new Bootstrap()
 *     .group(group)
 *     .channel(SharedMemChannel.class)
 *     .option(SharedMemChannelOption.REGION_CAPACITY, 4 * 1024 * 1024)
 *     .option(SharedMemChannelOption.POLL_INTERVAL_NS, 50_000L)
 *     .handler(new ChannelInitializer<SharedMemChannel>() {
 *         protected void initChannel(SharedMemChannel ch) {
 *             ch.pipeline().addLast(new MyHandler());
 *         }
 *     });
 * b.connect(new InetSocketAddress("my-service", 9000)).sync();
 * }</pre>
 */
public class SharedMemChannel extends AbstractSharedMemChannel {
    private static final InternalLogger LOG =
            InternalLoggerFactory.getInstance(SharedMemChannel.class);

    private final SharedMemChannelConfig config;

    /** Set by doBeginRead(); cleared each poll cycle when autoRead is off. */
    private volatile boolean readPending;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────────────────

    /** No-arg constructor used by Netty bootstrap reflection. */
    public SharedMemChannel() {
        this(null);
    }

    /**
     * @param parent the accepting {@link SharedMemServerChannel}, or {@code null}
     *               for a standalone client channel
     */
    public SharedMemChannel(Channel parent) {
        super(parent);
        this.config = new SharedMemChannelConfig(this);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Channel interface
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public SharedMemChannelConfig config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        SharedMemRegion tx = txRegion;
        SharedMemRegion rx = rxRegion;
        return (tx == null || !tx.isClosed()) && (rx == null || !rx.isClosed());
    }

    @Override
    public boolean isActive() {
        SharedMemRegion tx = txRegion;
        SharedMemRegion rx = rxRegion;
        return active && tx != null && rx != null && !tx.isClosed() && !rx.isClosed();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Performs the full client-side connection handshake:
     * <ol>
     *   <li>Generates a unique data-region name for this connection.</li>
     *   <li>Creates the data region (the client always owns region creation).</li>
     *   <li>Opens the server's CQ region and posts a {@link SharedMemConnectionToken}.</li>
     *   <li>Marks the channel active and fires {@code channelActive}.</li>
     * </ol>
     *
     * <p>The server will open this channel's data region from its side when it
     * processes the token in {@link SharedMemServerChannel#pollAccept()}.
     *
     * @param remoteAddr target server {@link InetSocketAddress}
     * @param localAddr  optional local address hint; if null, port 0 is used
     * @throws Exception if the data region or CQ region cannot be accessed
     */
    @Override
    protected void doConnect(SocketAddress remoteAddr, SocketAddress localAddr) throws Exception {
        if (!(remoteAddr instanceof InetSocketAddress)) {
            throw new IllegalArgumentException(
                    "SharedMemChannel requires remoteAddress to be InetSocketAddress, got: "
                            + (remoteAddr == null ? "null" : remoteAddr.getClass().getName()));
        }

        InetSocketAddress remote = (InetSocketAddress) remoteAddr;
        String host = remote.getHostString();
        String serverRegion = SharedMemEndpointKey.endpointKey(remote);

        // client id
        int clientPort = (localAddr instanceof InetSocketAddress)
                ? ((InetSocketAddress) localAddr).getPort()
                : 0;

        // unique connection region
        String uniqueDataRegion = serverRegion
                + "_" + clientPort
                + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        // IMPORTANT: keep original socket addresses for Spark compatibility
        this.remoteAddress = remoteAddr;
        this.localAddress  = new InetSocketAddress(host, clientPort);

        // ── Step 3: create the client→server tx region (client writes) ──────
        int capacity = config.getRegionSize();
        this.txRegion = new SharedMemRegion(uniqueDataRegion, capacity, true);

        // ── Step 4: open the server's CQ region and post the token ───────────
        String cqName = SharedMemEndpointKey.cqRegionName(remote);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting via server connection queue region: {}", cqName);
        }
        SharedMemRegion cqRegion = null;
        try {
            cqRegion = new SharedMemRegion(cqName, 1, false);
            SharedMemConnectionToken token =
                    new SharedMemConnectionToken(uniqueDataRegion, clientPort, capacity);
            byte[] encoded = token.encode();
            int written = cqRegion.write(encoded, 0, encoded.length);
            if (written < encoded.length) {
                throw new IllegalStateException(
                        "Server CQ region is full; could not post connection token.");
            }
        } finally {
            if (cqRegion != null && !cqRegion.isClosed()) {
                cqRegion.close();
            }
        }

        // ── Step 5: wait for server to create the server→client rx region ────
        this.rxRegion = waitForS2CRegion(uniqueDataRegion + S2C_SUFFIX, capacity);

        // ── Step 6: mark active and notify pipeline ───────────────────────────
        this.active = true;
        if (LOG.isInfoEnabled()) {
            LOG.info("Connected sharedmem channel local={} remote={} region={}",
                    localAddress, remoteAddress, uniqueDataRegion);
        }
        pipeline().fireChannelActive();
    }

    @Override
    protected void doDisconnect() throws Exception {
        doClose();
    }

    /** Client channels do not bind. */
    @Override
    protected void doBind(SocketAddress localAddress) {
        throw new UnsupportedOperationException(
                "SharedMemChannel does not support bind(). " +
                        "Use SharedMemServerChannel for the server side.");
    }

    /** Arms read interest for the next event-loop poll. */
    @Override
    protected void doBeginRead() {
        readPending = true;
    }

    /** Drains the pending outbound buffer into the data region. */
    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        writeToRegion(in);
    }

    @Override
    protected void doClose() throws Exception {
        active = false;
        super.doClose();           // closes the data region
        pipeline().fireChannelInactive();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event-loop integration
    // ─────────────────────────────────────────────────────────────────────────

    public boolean hasReadableData() {
        SharedMemRegion rx = rxRegion;
        return rx != null && !rx.isClosed() && rx.readableBytes() > 0;
    }

    private SharedMemRegion waitForS2CRegion(String name, int capacity) throws IOException {
        java.nio.file.Path path = SharedMemRegion.resolveBackingFile(name);
        long deadline = System.nanoTime() + 5_000_000_000L; // 5s
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for S2C region: {}", name);
        }
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return new SharedMemRegion(name, capacity, false);
            }
            try { Thread.sleep(1); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for server region: " + name);
            }
        }
        throw new IOException("Timeout waiting for server response region: " + name);
    }

    /**
     * Called by {@link SharedMemEventLoop} on every iteration.
     * Dispatches incoming data from the ring buffer to the pipeline only when
     * read interest is armed and the channel is active.
     */
    public void pollRead() {
        if (!isActive()) {
            return;
        }
        if (!readPending && !config.isAutoRead()) {
            return;
        }
        if (!config.isAutoRead()) {
            readPending = false; // consume arm; pipeline calls read() to re-arm
        }
        readFromRegion(config.getMaxMessagesPerRead());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    // private static SharedMemAddress requireSharedMemAddress(SocketAddress addr, String name) {
    //     if (addr instanceof SharedMemAddress) {
    //         return (SharedMemAddress) addr;
    //     }
    //     throw new IllegalArgumentException(
    //             name + " must be a SharedMemAddress, got: " +
    //                     (addr == null ? "null" : addr.getClass().getName()));
    // }
    @Override
    protected SocketAddress localAddress0() {
        return localAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return remoteAddress;
    }

    public SharedMemRegion getTxRegion() {
        return txRegion;
    }

    public SharedMemRegion getRxRegion() {
        return rxRegion;
    }

    private static String serverRegionName(SocketAddress remoteAddr) {
        if (remoteAddr instanceof SharedMemAddress) {
            return ((SharedMemAddress) remoteAddr).getRegionName();
        }
        if (remoteAddr instanceof InetSocketAddress) {
            InetSocketAddress inet = (InetSocketAddress) remoteAddr;
            return inet.getHostString() + "_" + inet.getPort();
        }
        throw new IllegalArgumentException(
                "remoteAddr must be InetSocketAddress or SharedMemAddress, got: " +
                        (remoteAddr == null ? "null" : remoteAddr.getClass().getName()));
    }
}
