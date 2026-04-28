package io.netty.channel.sharedmem;

import java.net.InetSocketAddress;

/**
 * Utility methods for converting {@link InetSocketAddress} values into
 * canonical shared-memory region namespace keys.
 */
final class SharedMemEndpointKey {

    private SharedMemEndpointKey() {
    }

    static String endpointKey(InetSocketAddress address) {
        return address.getHostString() + "_" + address.getPort();
    }

    static String cqRegionName(InetSocketAddress address) {
        return endpointKey(address) + SharedMemServerChannel.CQ_SUFFIX;
    }
}
