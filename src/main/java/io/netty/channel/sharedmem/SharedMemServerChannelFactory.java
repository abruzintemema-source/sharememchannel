package io.netty.channel.sharedmem;

import io.netty.channel.ChannelFactory;

/**
 * A {@link ChannelFactory} that creates {@link SharedMemServerChannel} instances.
 *
 * <pre>{@code
 * ServerBootstrap sb = new ServerBootstrap()
 *     .channelFactory(SharedMemServerChannelFactory.INSTANCE);
 * }</pre>
 */
public final class SharedMemServerChannelFactory implements ChannelFactory<SharedMemServerChannel> {

    /** Singleton instance — the factory is stateless. */
    public static final SharedMemServerChannelFactory INSTANCE = new SharedMemServerChannelFactory();

    private SharedMemServerChannelFactory() { }

    @Override
    public SharedMemServerChannel newChannel() {
        return new SharedMemServerChannel();
    }

    @Override
    public String toString() {
        return SharedMemServerChannelFactory.class.getSimpleName() + ".INSTANCE";
    }
}
