package io.netty.channel.sharedmem;

import io.netty.channel.ChannelFactory;

/**
 * A {@link ChannelFactory} that creates {@link SharedMemChannel} instances.
 *
 * <p>You typically don't need to use this directly — simply pass
 * {@code SharedMemChannel.class} (or {@code SharedMemServerChannel.class}) to the
 * bootstrap's {@code channel()} method:
 *
 * <pre>{@code
 * Bootstrap b = new Bootstrap()
 *     .channel(SharedMemChannel.class);        // uses reflection internally
 * }</pre>
 *
 * Use {@code SharedMemChannelFactory} when you need programmatic control:
 *
 * <pre>{@code
 * Bootstrap b = new Bootstrap()
 *     .channelFactory(new SharedMemChannelFactory());
 * }</pre>
 */
public final class SharedMemChannelFactory implements ChannelFactory<SharedMemChannel> {

    /** Singleton instance — the factory is stateless. */
    public static final SharedMemChannelFactory INSTANCE = new SharedMemChannelFactory();

    private SharedMemChannelFactory() { }

    @Override
    public SharedMemChannel newChannel() {
        return new SharedMemChannel();
    }

    @Override
    public String toString() {
        return SharedMemChannelFactory.class.getSimpleName() + ".INSTANCE";
    }
}
