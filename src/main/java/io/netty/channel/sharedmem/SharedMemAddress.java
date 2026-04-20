package io.netty.channel.sharedmem;

import java.net.SocketAddress;
import java.util.Objects;

/**
 * A {@link SocketAddress} implementation representing a SharedMem protocol endpoint.
 * <p>
 * A SharedMem address is identified by a region name (the shared memory segment key)
 * and an optional node identifier for multi-node topologies.
 */
public final class SharedMemAddress extends SocketAddress {

    private static final long serialVersionUID = 1L;

    /** The shared memory region name (acts as the "address"). */
    private final String regionName;

    /** Optional node/partition identifier within the region. */
    private final int nodeId;

    /**
     * Creates a new {@link SharedMemAddress} with the given region name and default node ID 0.
     *
     * @param regionName the name of the shared memory region
     */
    public SharedMemAddress(String regionName) {
        this(regionName, 0);
    }

    /**
     * Creates a new {@link SharedMemAddress} with the given region name and node ID.
     *
     * @param regionName the name of the shared memory region
     * @param nodeId     the node/partition identifier within the region
     */
    public SharedMemAddress(String regionName, int nodeId) {
        if (regionName == null || regionName.isEmpty()) {
            throw new IllegalArgumentException("regionName must not be null or empty");
        }
        this.regionName = regionName;
        this.nodeId = nodeId;
    }

    /**
     * Returns the shared memory region name.
     */
    public String getRegionName() {
        return regionName;
    }

    /**
     * Returns the node ID within the shared memory region.
     */
    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "sharedmem://" + regionName + "/" + nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SharedMemAddress)) return false;
        SharedMemAddress that = (SharedMemAddress) o;
        return nodeId == that.nodeId && Objects.equals(regionName, that.regionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionName, nodeId);
    }
}
