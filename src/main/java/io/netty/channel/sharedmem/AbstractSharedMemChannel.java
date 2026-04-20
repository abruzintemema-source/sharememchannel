package io.netty.channel.sharedmem;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.EventLoop;

import java.net.SocketAddress;

/**
 * Abstract base class for SharedMem channels.
 */
public abstract class AbstractSharedMemChannel extends AbstractChannel {

    protected static final ChannelMetadata METADATA = new ChannelMetadata(false);

    protected volatile SharedMemAddress localAddress;
    protected volatile SharedMemAddress remoteAddress;
    protected volatile boolean          registered;

    protected AbstractSharedMemChannel(Channel parent) {
        super(parent);
    }

    @Override
    public ChannelMetadata metadata() { return METADATA; }

    @Override
    protected abstract AbstractUnsafe newUnsafe();

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

    @Override protected abstract void doBind(SocketAddress localAddress) throws Exception;
    @Override protected abstract void doDisconnect() throws Exception;
    @Override protected abstract void doClose() throws Exception;
    @Override protected abstract void doWrite(ChannelOutboundBuffer in) throws Exception;

    // Fix: covariant return type — subclasses must return SharedMemChannelConfig.
    //      This makes config() consistent across the hierarchy and avoids the
    //      "return type ChannelConfig is not compatible with SharedMemChannelConfig" error
    //      that occurs when SharedMemServerChannel tried to return a plain ChannelConfig.
    @Override
    public abstract SharedMemChannelConfig config();

    @Override public abstract boolean isOpen();
    @Override public abstract boolean isActive();

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