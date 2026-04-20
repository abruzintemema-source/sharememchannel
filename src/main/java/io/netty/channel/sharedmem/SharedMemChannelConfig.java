package io.netty.channel.sharedmem;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;

import java.util.Map;

/**
 * Configuration for {@link SharedMemChannel} instances.
 * <p>
 * Exposes SharedMem-specific options via {@link SharedMemChannelOption} in addition
 * to all standard Netty {@link ChannelConfig} options.
 */
public class SharedMemChannelConfig extends DefaultChannelConfig {

    /** Default shared memory region size: 4 MB */
    public static final int DEFAULT_REGION_SIZE = 4 * 1024 * 1024;

    /** Default polling interval when ring is empty (microseconds). */
    public static final int DEFAULT_POLL_INTERVAL_US = 100;

    /** Default maximum number of messages read per event-loop tick. */
    public static final int DEFAULT_MAX_MESSAGES_PER_READ = 16;

    private volatile int  regionSize         = DEFAULT_REGION_SIZE;
    private volatile int  pollIntervalUs      = DEFAULT_POLL_INTERVAL_US;
    private volatile int  maxMessagesPerRead  = DEFAULT_MAX_MESSAGES_PER_READ;
    private volatile boolean directBuffer     = true;

    public SharedMemChannelConfig(AbstractSharedMemChannel channel) {
        super(channel);
    }

    // -------------------------------------------------------------------------
    // ChannelConfig overrides
    // -------------------------------------------------------------------------

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return getOptions(
                super.getOptions(),
                SharedMemChannelOption.REGION_SIZE,
                SharedMemChannelOption.POLL_INTERVAL_US,
                SharedMemChannelOption.MAX_MESSAGES_PER_READ,
                SharedMemChannelOption.DIRECT_BUFFER
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOption(ChannelOption<T> option) {
        if (option == SharedMemChannelOption.REGION_SIZE) {
            return (T) Integer.valueOf(getRegionSize());
        }
        if (option == SharedMemChannelOption.POLL_INTERVAL_US) {
            return (T) Integer.valueOf(getPollIntervalUs());
        }
        if (option == SharedMemChannelOption.MAX_MESSAGES_PER_READ) {
            return (T) Integer.valueOf(getMaxMessagesPerRead());
        }
        if (option == SharedMemChannelOption.DIRECT_BUFFER) {
            return (T) Boolean.valueOf(isDirectBuffer());
        }
        return super.getOption(option);
    }

    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        validate(option, value);
        if (option == SharedMemChannelOption.REGION_SIZE) {
            setRegionSize((Integer) value);
        } else if (option == SharedMemChannelOption.POLL_INTERVAL_US) {
            setPollIntervalUs((Integer) value);
        } else if (option == SharedMemChannelOption.MAX_MESSAGES_PER_READ) {
            setMaxMessagesPerRead((Integer) value);
        } else if (option == SharedMemChannelOption.DIRECT_BUFFER) {
            setDirectBuffer((Boolean) value);
        } else {
            return super.setOption(option, value);
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the size (bytes) of the shared memory region.
     */
    public int getRegionSize() {
        return regionSize;
    }

    /**
     * Sets the size (bytes) of the shared memory region.
     * Must be set before the channel is registered.
     */
    public SharedMemChannelConfig setRegionSize(int regionSize) {
        if (regionSize <= SharedMemRegion.HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "regionSize must be > " + SharedMemRegion.HEADER_SIZE);
        }
        this.regionSize = regionSize;
        return this;
    }

    /**
     * Returns the busy-poll interval in microseconds used when the ring is empty.
     */
    public int getPollIntervalUs() {
        return pollIntervalUs;
    }

    public SharedMemChannelConfig setPollIntervalUs(int pollIntervalUs) {
        if (pollIntervalUs < 0) {
            throw new IllegalArgumentException("pollIntervalUs must be >= 0");
        }
        this.pollIntervalUs = pollIntervalUs;
        return this;
    }

    /**
     * Returns the maximum number of messages read per event-loop iteration.
     */
    public int getMaxMessagesPerRead() {
        return maxMessagesPerRead;
    }

    public SharedMemChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
        if (maxMessagesPerRead <= 0) {
            throw new IllegalArgumentException("maxMessagesPerRead must be > 0");
        }
        this.maxMessagesPerRead = maxMessagesPerRead;
        return this;
    }

    /**
     * Whether to use direct (off-heap) ByteBuffers for ring I/O.
     */
    public boolean isDirectBuffer() {
        return directBuffer;
    }

    public SharedMemChannelConfig setDirectBuffer(boolean directBuffer) {
        this.directBuffer = directBuffer;
        return this;
    }

    // -------------------------------------------------------------------------
    // Delegate fluent setters (return SharedMemChannelConfig for chaining)
    // -------------------------------------------------------------------------

    @Override
    public SharedMemChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
        super.setConnectTimeoutMillis(connectTimeoutMillis);
        return this;
    }

    @Override
    public SharedMemChannelConfig setAllocator(ByteBufAllocator allocator) {
        super.setAllocator(allocator);
        return this;
    }

    @Override
    public SharedMemChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
        super.setRecvByteBufAllocator(allocator);
        return this;
    }

    @Override
    public SharedMemChannelConfig setAutoRead(boolean autoRead) {
        super.setAutoRead(autoRead);
        return this;
    }

    @Override
    public SharedMemChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark) {
        super.setWriteBufferWaterMark(writeBufferWaterMark);
        return this;
    }

    @Override
    public SharedMemChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
        super.setMessageSizeEstimator(estimator);
        return this;
    }
}