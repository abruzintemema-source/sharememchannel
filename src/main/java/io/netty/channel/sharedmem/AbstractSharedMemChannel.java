package io.netty.channel.sharedmem;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;

import java.net.SocketAddress;

/**
 * Abstract base class for SharedMem channels.
 */
public abstract class AbstractSharedMemChannel extends AbstractChannel {

    protected static final ChannelMetadata METADATA = new ChannelMetadata(false);

    /** Suffix appended to the client data-region name for the server→client ring. */
    static final String S2C_SUFFIX = ".s2c";

    protected volatile SharedMemAddress localAddress;
    protected volatile SharedMemAddress remoteAddress;
    protected volatile boolean          registered;
    protected volatile boolean          active;
    /** Ring we write into (our outbound direction). */
    protected volatile SharedMemRegion  txRegion;
    /** Ring we read from (our inbound direction). */
    protected volatile SharedMemRegion  rxRegion;

    protected AbstractSharedMemChannel(Channel parent) {
        super(parent);
    }

    @Override
    public ChannelMetadata metadata() { return METADATA; }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return loop instanceof SharedMemEventLoop;
    }

    @Override
    protected SocketAddress localAddress0()  { return localAddress; }

    @Override
    protected SocketAddress remoteAddress0() { return remoteAddress; }

    @Override
    protected void doRegister() throws Exception {
        registered = true;
        ((SharedMemEventLoop) eventLoop()).addChannel(this);
    }

    @Override
    protected void doDeregister() throws Exception {
        registered = false;
        ((SharedMemEventLoop) eventLoop()).removeChannel(this);
    }

    @Override
    protected void doBeginRead() throws Exception {
        // polling-driven — nothing to do
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new AbstractUnsafe() {
            @Override
            public void connect(SocketAddress remoteAddress, SocketAddress localAddress,
                                ChannelPromise promise) {
                if (!promise.setUncancellable() || !ensureOpen(promise)) {
                    return;
                }
                try {
                    doConnect(remoteAddress, localAddress);
                    safeSetSuccess(promise);
                } catch (Exception e) {
                    safeSetFailure(promise, e);
                    closeIfClosed();
                }
            }
        };
    }

    protected void doConnect(SocketAddress remoteAddress, SocketAddress localAddress)
            throws Exception {
        throw new UnsupportedOperationException("doConnect");
    }

    @Override protected abstract void doBind(SocketAddress localAddress) throws Exception;
    @Override protected abstract void doDisconnect() throws Exception;

    @Override
    protected void doClose() throws Exception {
        if (txRegion != null && !txRegion.isClosed()) {
            txRegion.close();
        }
        if (rxRegion != null && !rxRegion.isClosed()) {
            rxRegion.close();
        }
    }

    @Override protected abstract void doWrite(ChannelOutboundBuffer in) throws Exception;

    @Override
    public abstract SharedMemChannelConfig config();

    @Override public abstract boolean isOpen();
    @Override public abstract boolean isActive();

    protected void readFromRegion(int maxMessages) {
        SharedMemRegion rx = rxRegion;
        if (rx == null) return;
        int messagesRead = 0;
        while (messagesRead < maxMessages) {
            int available = rx.readableBytes();
            if (available <= 0) break;
            byte[] bytes = new byte[available];
            int read = rx.read(bytes, 0, available);
            if (read <= 0) break;
            ByteBuf buf = config().getAllocator().buffer(read);
            buf.writeBytes(bytes, 0, read);
            pipeline().fireChannelRead(buf);
            messagesRead++;
        }
        if (messagesRead > 0) {
            pipeline().fireChannelReadComplete();
        }
    }

    protected void writeToRegion(ChannelOutboundBuffer in) throws Exception {
        SharedMemRegion tx = txRegion;
        if (tx == null) return;
        for (;;) {
            Object msg = in.current();
            if (msg == null) break;
            if (!(msg instanceof ByteBuf)) {
                in.remove(new UnsupportedOperationException("Only ByteBuf messages are supported"));
                continue;
            }
            ByteBuf buf = (ByteBuf) msg;
            int len = buf.readableBytes();
            if (tx.writableBytes() < len) break;
            byte[] bytes = new byte[len];
            buf.getBytes(buf.readerIndex(), bytes);
            int written = tx.write(bytes, 0, len);
            if (written < len) break;
            in.remove();
        }
    }

    protected java.nio.ByteBuffer allocateIoBuffer(int capacity) {
        SharedMemChannelConfig cfg = config();
        return cfg.isDirectBuffer()
                ? java.nio.ByteBuffer.allocateDirect(capacity)
                : java.nio.ByteBuffer.allocate(capacity);
    }

    protected java.nio.ByteBuffer toNioBuffer(ByteBuf buf) {
        if (buf.nioBufferCount() == 1) return buf.nioBuffer();
        java.nio.ByteBuffer nio = allocateIoBuffer(buf.readableBytes());
        buf.readBytes(nio);
        nio.flip();
        return nio;
    }
}