package io.netty.channel.sharedmem;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A full-duplex Netty {@link Channel} backed by a {@link SharedMemRegion} ring buffer.
 */
public class SharedMemChannel extends AbstractSharedMemChannel {

    private static final InternalLogger LOG =
            InternalLoggerFactory.getInstance(SharedMemChannel.class);

    private static final AtomicInteger OPEN_COUNT = new AtomicInteger();

    private final SharedMemChannelConfig config;

    private volatile SharedMemRegion txRegion;
    private volatile SharedMemRegion rxRegion;

    private volatile boolean open   = true;
    private volatile boolean active = false;

    private ByteBuffer readBuffer;

    public SharedMemChannel() {
        super(null);
        this.config = new SharedMemChannelConfig(this);
    }

    SharedMemChannel(Channel parent,
                     SharedMemRegion txRegion,
                     SharedMemRegion rxRegion,
                     SharedMemAddress remoteAddr) {
        super(parent);
        this.config        = new SharedMemChannelConfig(this);
        this.txRegion      = txRegion;
        this.rxRegion      = rxRegion;
        this.remoteAddress = remoteAddr;
        this.active        = true;
        this.readBuffer    = allocateIoBuffer(rxRegion.getDataSize());
        OPEN_COUNT.incrementAndGet();
    }

    @Override public boolean isOpen()   { return open; }
    @Override public boolean isActive() { return open && active; }
    @Override public SharedMemChannelConfig config() { return config; }

    // -------------------------------------------------------------------------
    // Bind / Connect / Disconnect / Close
    // -------------------------------------------------------------------------

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (!(localAddress instanceof SharedMemAddress)) {
            throw new ChannelException("Unsupported address type: " + localAddress.getClass());
        }
        this.localAddress = (SharedMemAddress) localAddress;
    }

    protected void doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
        if (!(remoteAddress instanceof SharedMemAddress)) {
            throw new ChannelException("Unsupported address type: " + remoteAddress.getClass());
        }
        SharedMemAddress remote = (SharedMemAddress) remoteAddress;
        SharedMemAddress local  = localAddress instanceof SharedMemAddress
                ? (SharedMemAddress) localAddress
                : new SharedMemAddress("client_" + OPEN_COUNT.incrementAndGet());

        this.localAddress  = local;
        this.remoteAddress = remote;

        int regionSize = config.getRegionSize();
        String txName  = local.getRegionName() + "_to_" + remote.getRegionName();
        String rxName  = remote.getRegionName() + "_to_" + local.getRegionName();

        this.txRegion   = new SharedMemRegion(txName, regionSize);
        this.rxRegion   = new SharedMemRegion(rxName, regionSize);
        this.readBuffer = allocateIoBuffer(rxRegion.getDataSize());
        this.active     = true;
        OPEN_COUNT.incrementAndGet();
        LOG.info("Connected: {} <-> {} (tx={}, rx={})", local, remote, txName, rxName);
    }

    @Override
    protected void doDisconnect() throws Exception { doClose(); }

    @Override
    protected void doClose() throws Exception {
        open   = false;
        active = false;
        try {
            if (txRegion != null) { txRegion.close(); txRegion = null; }
            if (rxRegion != null) { rxRegion.close(); rxRegion = null; }
        } catch (IOException e) {
            LOG.warn("Error closing SharedMem regions", e);
        }
        OPEN_COUNT.decrementAndGet();
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        if (txRegion == null) throw new ChannelException("Channel not connected");

        int flushed = 0;
        int max     = config.getMaxMessagesPerRead();

        while (flushed < max) {
            Object msg = in.current();
            if (msg == null) break;
            if (!(msg instanceof ByteBuf)) {
                in.remove(new UnsupportedOperationException("Unsupported msg type: " + msg.getClass()));
                continue;
            }
            ByteBuffer nio = toNioBuffer((ByteBuf) msg);
            if (!txRegion.write(nio)) break; // ring full
            in.remove();
            flushed++;
        }
    }

    // -------------------------------------------------------------------------
    // Read (called by event loop)
    // -------------------------------------------------------------------------

    int doReadMessages() {
        if (rxRegion == null || !active) return 0;

        ChannelPipeline pipeline = pipeline();
        int read = 0;

        while (read < config.getMaxMessagesPerRead() && rxRegion.isReadable()) {
            readBuffer.clear();
            int n = rxRegion.read(readBuffer);
            if (n <= 0) break;

            ByteBuf buf = config.getAllocator().buffer(n);
            readBuffer.position(0);
            readBuffer.limit(n);
            buf.writeBytes(readBuffer);
            pipeline.fireChannelRead(buf);
            read++;
        }

        if (read > 0) pipeline.fireChannelReadComplete();
        return read;
    }

    boolean hasReadableData() {
        return rxRegion != null && rxRegion.isReadable();
    }

    // -------------------------------------------------------------------------
    // Unsafe
    // Fix: AbstractUnsafe.bind() is final — do NOT override it.
    //      Bind is handled via doBind() called by the AbstractUnsafe.bind() final impl.
    //      Only override connect() which is not final.
    // -------------------------------------------------------------------------

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new SharedMemUnsafe();
    }

    private final class SharedMemUnsafe extends AbstractUnsafe {

        @Override
        public void connect(SocketAddress remoteAddress,
                            SocketAddress localAddress,
                            ChannelPromise promise) {
            if (!promise.setUncancellable()) return;
            if (!isOpen()) {
                promise.tryFailure(new ChannelException("Channel is closed"));
                return;
            }
            try {
                doConnect(remoteAddress, localAddress);
                promise.setSuccess();
                pipeline().fireChannelActive();
            } catch (Throwable t) {
                promise.tryFailure(t);
                closeIfClosed();
            }
        }
        // NOTE: do NOT override bind() — it is final in AbstractUnsafe.
        //       AbstractUnsafe.bind() calls our doBind() automatically.
    }
}