package io.netty.channel.sharedmem;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A server-side SharedMem channel that listens for inbound connections.
 *
 * Fix: implements {@link ServerChannel} so it is accepted by ServerBootstrap.
 * Fix: config() returns {@link SharedMemChannelConfig} to satisfy the abstract method
 *      declared in {@link AbstractSharedMemChannel}.
 */
public class SharedMemServerChannel extends AbstractSharedMemChannel implements ServerChannel {

    private static final InternalLogger LOG =
            InternalLoggerFactory.getInstance(SharedMemServerChannel.class);

    // Server channel uses the same SharedMemChannelConfig as data channels.
    // The regionSize here is used when accepting child channels.
    private final SharedMemChannelConfig config;

    private final Set<String> acceptedRegions = new HashSet<>();

    private volatile boolean open   = true;
    private volatile boolean active = false;

    public SharedMemServerChannel() {
        super(null);
        this.config = new SharedMemChannelConfig(this);
    }

    @Override public boolean isOpen()   { return open; }
    @Override public boolean isActive() { return active; }

    // Fix: must return SharedMemChannelConfig, not ChannelConfig,
    //      to match the covariant return declared in AbstractSharedMemChannel.
    @Override
    public SharedMemChannelConfig config() { return config; }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (!(localAddress instanceof SharedMemAddress)) {
            throw new ChannelException("Unsupported address type: " + localAddress.getClass());
        }
        this.localAddress = (SharedMemAddress) localAddress;
        this.active = true;
        LOG.info("SharedMemServerChannel listening on {}", localAddress);
    }

    @Override
    protected void doDisconnect() {
        throw new UnsupportedOperationException("Server channels cannot disconnect");
    }

    @Override
    protected void doClose() throws Exception {
        active = false;
        open   = false;
        LOG.info("SharedMemServerChannel closed: {}", localAddress);
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) {
        throw new UnsupportedOperationException("Server channels cannot write");
    }

    // -------------------------------------------------------------------------
    // Accept path (called by event loop)
    // -------------------------------------------------------------------------

    int doReadMessages() throws Exception {
        if (!active || localAddress == null) return 0;

        String serverRegion = localAddress.getRegionName();
        String suffix       = "_to_" + serverRegion + ".shm";

        Path shmDir = Paths.get(System.getProperty("sharedmem.dir",
                System.getProperty("java.io.tmpdir") + "/sharedmem"));

        if (!Files.exists(shmDir)) return 0;

        List<String> newRegions = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(shmDir, "*" + suffix)) {
            for (Path p : stream) {
                String fileName   = p.getFileName().toString();
                String regionName = fileName.substring(0, fileName.length() - 4);
                if (!acceptedRegions.contains(regionName)) {
                    newRegions.add(regionName);
                }
            }
        } catch (IOException e) {
            LOG.warn("Error scanning shm directory", e);
            return 0;
        }

        int accepted   = 0;
        int regionSize = config.getRegionSize();

        for (String rxName : newRegions) {
            String clientRegion = rxName.substring(0, rxName.indexOf("_to_"));
            String txName       = serverRegion + "_to_" + clientRegion;
            SharedMemAddress remoteAddr = new SharedMemAddress(clientRegion);

            try {
                SharedMemRegion rx = new SharedMemRegion(rxName, regionSize);
                SharedMemRegion tx = new SharedMemRegion(txName, regionSize);

                SharedMemChannel child = new SharedMemChannel(this, tx, rx, remoteAddr);
                child.localAddress = localAddress;

                acceptedRegions.add(rxName);
                pipeline().fireChannelRead(child);
                accepted++;
                LOG.info("Accepted connection from {}", clientRegion);
            } catch (Exception e) {
                LOG.warn("Failed to accept from region {}", rxName, e);
            }
        }

        if (accepted > 0) pipeline().fireChannelReadComplete();
        return accepted;
    }

    boolean hasReadableData() {
        return active;
    }

    // -------------------------------------------------------------------------
    // Unsafe
    // -------------------------------------------------------------------------

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new SharedMemServerUnsafe();
    }

    private final class SharedMemServerUnsafe extends AbstractUnsafe {
        @Override
        public void connect(SocketAddress remoteAddress,
                            SocketAddress localAddress,
                            ChannelPromise promise) {
            promise.setFailure(new UnsupportedOperationException("Server channels cannot connect"));
        }
    }
}