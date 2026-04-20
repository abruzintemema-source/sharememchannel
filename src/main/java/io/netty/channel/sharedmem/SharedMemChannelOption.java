package io.netty.channel.sharedmem;

import io.netty.channel.ChannelOption;

/**
 * SharedMem-specific {@link ChannelOption} definitions.
 *
 * <pre>
 * Bootstrap b = new Bootstrap();
 * b.option(SharedMemChannelOption.REGION_SIZE,        8 * 1024 * 1024)  // 8 MB ring
 *  .option(SharedMemChannelOption.POLL_INTERVAL_US,   50)               // 50 µs poll
 *  .option(SharedMemChannelOption.MAX_MESSAGES_PER_READ, 32)
 *  .option(SharedMemChannelOption.DIRECT_BUFFER,      true);
 * </pre>
 */
public final class SharedMemChannelOption {

    /**
     * The size (bytes) of the memory-mapped ring buffer region.
     * Default: {@value SharedMemChannelConfig#DEFAULT_REGION_SIZE}.
     */
    public static final ChannelOption<Integer> REGION_SIZE =
            ChannelOption.valueOf("SHAREDMEM_REGION_SIZE");

    /**
     * Busy-poll sleep interval in microseconds when the ring is empty.
     * Set to 0 for pure spin. Default: {@value SharedMemChannelConfig#DEFAULT_POLL_INTERVAL_US}.
     */
    public static final ChannelOption<Integer> POLL_INTERVAL_US =
            ChannelOption.valueOf("SHAREDMEM_POLL_INTERVAL_US");

    /**
     * Maximum number of messages dequeued per event-loop iteration.
     * Default: {@value SharedMemChannelConfig#DEFAULT_MAX_MESSAGES_PER_READ}.
     */
    public static final ChannelOption<Integer> MAX_MESSAGES_PER_READ =
            ChannelOption.valueOf("SHAREDMEM_MAX_MESSAGES_PER_READ");

    /**
     * Whether to allocate direct (off-heap) ByteBuffers for ring read/write operations.
     * Default: {@code true}.
     */
    public static final ChannelOption<Boolean> DIRECT_BUFFER =
            ChannelOption.valueOf("SHAREDMEM_DIRECT_BUFFER");

    private SharedMemChannelOption() { }
}
