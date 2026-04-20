package io.netty.channel.sharedmem;

import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;

import java.net.SocketAddress;

/**
 * A Netty {@link ServerChannel} that listens on a {@link SharedMemAddress} and
 * accepts incoming {@link SharedMemChannel} connections via a shared-memory
 * connection-queue region.
 *
 * <h3>Correct lifecycle (analogous to TCP ServerSocketChannel)</h3>
 * <pre>
 *  SERVER                                      CLIENT
 *  ──────                                      ──────
 *  doBind()
 *    └─ creates CQ region only                 doConnect()
 *       (no data region yet,                     └─ 1. creates its own dedicated
 *        no child channels)                           data region  (client-owned)
 *                                                  2. writes SharedMemConnectionToken
 *  pollAccept()  ◄────────────── token ────────      into server's CQ region
 *    └─ reads token from CQ
 *    └─ opens client's data region (read-write)
 *    └─ creates SharedMemChannel child
 *    └─ fires channelRead(child) into pipeline
 * </pre>
 *
 * <h3>Key design invariants</h3>
 * <ul>
 *   <li>After {@code doBind()}, <b>no data region exists and no child channel
 *       is created</b>.  The server is purely passive — it only monitors the
 *       CQ region for incoming tokens.</li>
 *   <li>Each accepted child channel gets its <b>own dedicated data region</b>
 *       created by the client, identified by the region name in the token.
 *       Region names are unique per connection (e.g. {@code "svc_clientId_timestamp"}).</li>
 *   <li>The server never creates data regions — only the client does.  The
 *       server only opens (reads) a region whose backing file the client has
 *       already written.</li>
 *   <li>{@link #pollAccept()} produces child channels <b>only</b> when a
 *       well-formed token is present in the CQ.  An idle server loop produces
 *       nothing.</li>
 * </ul>
 *
 * <h3>Bootstrap example</h3>
 * <pre>{@code
 * EventLoopGroup boss   = new SharedMemEventLoopGroup(1);
 * EventLoopGroup worker = new SharedMemEventLoopGroup(4);
 *
 * ServerBootstrap b = new ServerBootstrap()
 *     .group(boss, worker)
 *     .channel(SharedMemServerChannel.class)
 *     .option(SharedMemChannelOption.POLL_INTERVAL_NS, 50_000L)
 *     .childHandler(new ChannelInitializer<SharedMemChannel>() {
 *         protected void initChannel(SharedMemChannel ch) {
 *             ch.pipeline().addLast(new MyHandler());
 *         }
 *     });
 *
 * b.bind(new SharedMemAddress("my-service", 9000)).sync();
 * }</pre>
 */
public class SharedMemServerChannel extends AbstractSharedMemChannel implements ServerChannel {

    private static final ChannelMetadata SERVER_METADATA = new ChannelMetadata(false);

    /**
     * Suffix appended to the server's region name to form the CQ region name.
     * Example: server bound to {@code "my-svc"} → CQ file is {@code "my-svc.cq"}.
     * The client must know this suffix to post tokens.
     */
    public static final String CQ_SUFFIX = ".cq";

    /**
     * Capacity of the CQ region. Each token is {@link SharedMemConnectionToken#ENCODED_SIZE}
     * bytes (128 bytes), so this fits 512 simultaneous pending connect requests.
     */
    private static final int CQ_CAPACITY = 512 * SharedMemConnectionToken.ENCODED_SIZE;

    // ─────────────────────────────────────────────────────────────────────────
    // State
    // ─────────────────────────────────────────────────────────────────────────

    private final SharedMemChannelConfig config;

    /**
     * The connection-queue region — the only region the server creates at bind time.
     * Clients write {@link SharedMemConnectionToken} entries here to request acceptance.
     * This is the SharedMem equivalent of the OS listen backlog queue.
     */
    private volatile SharedMemRegion connectionQueueRegion;

    /** Guards read interest — set by doBeginRead(), checked by pollAccept(). */
    private volatile boolean readPending;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public SharedMemServerChannel() {
        super(null /* server channels have no parent */);
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
    public ChannelMetadata metadata() {
        return SERVER_METADATA;
    }

    /**
     * The server channel is open as long as the CQ region has not been closed.
     * Note: {@link #region} is intentionally always null on the server channel —
     * the server never owns a data region.
     */
    @Override
    public boolean isOpen() {
        SharedMemRegion cq = connectionQueueRegion;
        return cq == null || !cq.isClosed();
    }

    /**
     * The server channel is active once it is bound (CQ region created and open).
     * It does NOT require any client to have connected.
     */
    @Override
    public boolean isActive() {
        SharedMemRegion cq = connectionQueueRegion;
        return active && cq != null && !cq.isClosed();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Binds the server channel.
     *
     * <p><b>Only the CQ region is created here.</b>  No data region, no child
     * channels.  This is analogous to {@code ServerSocketChannel.bind()} — the
     * server is now listening but no client has yet connected.
     *
     * <p>Any pre-existing CQ backing file is truncated so stale tokens from a
     * previous server run are discarded.
     *
     * @param localAddr must be a {@link SharedMemAddress}
     * @throws Exception if the CQ region backing file cannot be created
     */
    @Override
    protected void doBind(SocketAddress localAddr) throws Exception {
        SharedMemAddress addr = requireSharedMemAddress(localAddr, "localAddress");
        this.localAddress = addr;

        // Create ONLY the connection-queue region.
        // No data region is created here — clients create their own data regions.
        this.connectionQueueRegion = new SharedMemRegion(
                cqRegionName(addr),
                CQ_CAPACITY,
                true /* create / truncate — discard any stale tokens */);

        // Mark the server as active — it is now listening.
        // There are zero child channels at this point, which is correct.
        this.active = true;
    }

    @Override
    protected void doDisconnect() throws Exception {
        doClose();
    }

    /** Server channels never initiate connections. */
    @Override
    protected void doConnect(SocketAddress remoteAddress, SocketAddress localAddress) {
        throw new UnsupportedOperationException(
                "SharedMemServerChannel cannot connect. Use SharedMemChannel (client).");
    }

    /**
     * Closes the CQ region.  Child channels are closed independently by the worker loops.
     */
    @Override
    protected void doClose() throws Exception {
        active = false;
        SharedMemRegion cq = this.connectionQueueRegion;
        if (cq != null && !cq.isClosed()) {
            cq.close();
        }
        // Note: this.region is always null on the server channel — no super.doClose() needed
        // for a data region. We call it anyway for AbstractChannel bookkeeping.
        // AbstractSharedMemChannel.doClose() checks for null region, so it is safe.
        super.doClose();
    }

    /**
     * Arms the read-interest flag so the boss event-loop will call
     * {@link #pollAccept()} on the next iteration.
     */
    @Override
    protected void doBeginRead() {
        readPending = true;
    }

    /** Server channel never writes — child channels do. */
    @Override
    protected void doWrite(ChannelOutboundBuffer in) {
        throw new UnsupportedOperationException(
                "Cannot write to SharedMemServerChannel. Write to the child SharedMemChannel.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accept loop — called by SharedMemEventLoop
    // ─────────────────────────────────────────────────────────────────────────

    public boolean hasReadableData() {
        SharedMemRegion cq = connectionQueueRegion;
        return cq != null && !cq.isClosed()
                && cq.readableBytes() >= SharedMemConnectionToken.ENCODED_SIZE;
    }

    /**
     * Polls the CQ region for pending connection tokens.
     *
     * <p>This is the SharedMem equivalent of {@code ServerSocketChannel.accept()}.
     * It is called by the boss {@link SharedMemEventLoop} on every iteration.
     *
     * <p><b>Produces child channels only when a client has written a token.</b>
     * An idle CQ (no clients connected) results in zero child channels — exactly
     * as a TCP server produces no channels until a client calls {@code connect()}.
     *
     * <p>For each valid token found:
     * <ol>
     *   <li>Opens the client's data region (already created by the client).</li>
     *   <li>Creates a {@link SharedMemChannel} child backed by that region.</li>
     *   <li>Fires {@code channelRead(child)} through the server pipeline so
     *       Netty's {@code ServerBootstrap.ServerBootstrapAcceptor} can register
     *       the child with a worker event loop.</li>
     * </ol>
     *
     * <p>Must be called exclusively from the boss event-loop thread.
     */
    public void pollAccept() {
        // Guard 1: server must be bound and CQ open
        if (!isActive()) {
            return;
        }

        // Guard 2: read interest must be armed
        if (!readPending && !config.isAutoRead()) {
            return;
        }
        if (!config.isAutoRead()) {
            readPending = false; // consume the arm; pipeline must call read() again
        }

        SharedMemRegion cq = this.connectionQueueRegion;
        if (cq == null || cq.isClosed()) {
            return;
        }

        // Fast-path: nothing in the CQ at all — return immediately without
        // allocating any objects. This is the common case for an idle server.
        if (cq.readableBytes() < SharedMemConnectionToken.ENCODED_SIZE) {
            return;
        }

        ChannelPipeline pipeline = pipeline();
        boolean         accepted = false;

        try {
            while (cq.readableBytes() >= SharedMemConnectionToken.ENCODED_SIZE) {
                byte[] tokenBytes = new byte[SharedMemConnectionToken.ENCODED_SIZE];
                int    read       = cq.read(tokenBytes, 0, tokenBytes.length);

                if (read < SharedMemConnectionToken.ENCODED_SIZE) {
                    // Partial read — ring cursor is now corrupt; log and stop.
                    System.err.println(
                            "[SharedMemServerChannel] Partial token read (" + read +
                                    " bytes). CQ may be corrupt. Stopping accept loop.");
                    break;
                }

                SharedMemConnectionToken token;
                try {
                    token = SharedMemConnectionToken.decode(tokenBytes);
                } catch (IllegalArgumentException ex) {
                    System.err.println(
                            "[SharedMemServerChannel] Malformed token, skipping: " + ex.getMessage());
                    continue;
                }

                SharedMemChannel child = createChildChannel(token);
                pipeline.fireChannelRead(child);
                accepted = true;
            }

        } catch (Exception ex) {
            System.err.println(
                    "[SharedMemServerChannel] Exception in pollAccept: " + ex.getMessage());
            ex.printStackTrace(System.err);
        } finally {
            if (accepted) {
                pipeline.fireChannelReadComplete();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates an accepted {@link SharedMemChannel} child from a decoded token.
     *
     * <p>The client is responsible for having already created the data region
     * identified in the token <em>before</em> writing the token to the CQ.
     * This mirrors the TCP model: the client creates the socket and performs the
     * OS handshake before the server's {@code accept()} returns it.
     *
     * @param token decoded handshake token posted by the client
     * @return a new child channel backed by the client's data region
     * @throws Exception if the client's data region backing file cannot be opened
     */
    private SharedMemChannel createChildChannel(SharedMemConnectionToken token) throws Exception {
        SharedMemChannel child = new SharedMemChannel(this /* parent */);

        child.remoteAddress = new SharedMemAddress(token.getRegionName(), token.getPort());
        child.localAddress  = this.localAddress;

        // Server reads from client's tx region (client→server direction).
        child.rxRegion = new SharedMemRegion(
                token.getRegionName(),
                token.getRegionCapacity(),
                false /* open existing */);

        // Server creates the server→client tx region; client polls for this file.
        child.txRegion = new SharedMemRegion(
                token.getRegionName() + S2C_SUFFIX,
                token.getRegionCapacity(),
                true /* create */);

        child.active = true;
        return child;
    }

    /**
     * Returns the CQ region name for a given server address.
     * Exposed as package-private so {@link SharedMemChannel#doConnect} can
     * compute the same name when writing its token.
     */
    static String cqRegionName(SharedMemAddress serverAddress) {
        return serverAddress.getRegionName() + CQ_SUFFIX;
    }

    private static SharedMemAddress requireSharedMemAddress(SocketAddress addr, String name) {
        if (addr instanceof SharedMemAddress) {
            return (SharedMemAddress) addr;
        }
        throw new IllegalArgumentException(
                name + " must be a SharedMemAddress, got: " +
                        (addr == null ? "null" : addr.getClass().getName()));
    }
}