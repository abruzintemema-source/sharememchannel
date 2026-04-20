package io.netty.channel.sharedmem;

import io.netty.channel.ChannelException;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link SingleThreadEventLoop} that drives SharedMem channels.
 *
 * <p>Instead of a {@code Selector}, this loop maintains a registry of active
 * {@link AbstractSharedMemChannel} instances added via {@link #addChannel}.
 * Channels self-register in their {@code doRegister()} hook. On each iteration:
 * <ol>
 *   <li>Drains readable data from every registered channel.</li>
 *   <li>Runs all pending tasks submitted to the event loop.</li>
 *   <li>If no channels had data and busy-spin is off, sleeps briefly.</li>
 * </ol>
 *
 * <h3>Busy-spin mode</h3>
 * Set {@code -Dsharedmem.busySpin=true} to disable the idle sleep entirely and
 * achieve the absolute minimum latency at the cost of a full CPU core.
 */
public final class SharedMemEventLoop extends SingleThreadEventLoop {

    private static final InternalLogger LOG =
            InternalLoggerFactory.getInstance(SharedMemEventLoop.class);

    /** Nanoseconds to sleep when no channels have data (~100 µs default). */
    private static final long IDLE_SLEEP_NS =
            Long.getLong("sharedmem.idleSleepNs", 100_000L);

    /** When true, never sleep — pure spin for ultra-low latency. */
    private static final boolean BUSY_SPIN =
            Boolean.getBoolean("sharedmem.busySpin");

    /**
     * Maximum pending tasks passed to the super-class constructor.
     * SingleThreadEventExecutor.DEFAULT_MAX_PENDING_EXECUTOR_TASKS is package-private,
     * so we use the same documented default value of Integer.MAX_VALUE.
     */
    private static final int MAX_PENDING_TASKS = Integer.MAX_VALUE;

    /** All channels registered with this event loop. */
    private final Set<AbstractSharedMemChannel> channels =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicBoolean wakenUp = new AtomicBoolean();

    // -------------------------------------------------------------------------
    // Constructors (mirror NioEventLoop signatures for familiarity)
    // -------------------------------------------------------------------------

    SharedMemEventLoop(SharedMemEventLoopGroup parent,
                       ThreadFactory threadFactory,
                       SelectStrategyFactory selectStrategyFactory) {
        super(parent, threadFactory, false,
                MAX_PENDING_TASKS, RejectedExecutionHandlers.reject());
    }

    SharedMemEventLoop(SharedMemEventLoopGroup parent,
                       Executor executor,
                       SelectStrategyFactory selectStrategyFactory,
                       RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, false,
                MAX_PENDING_TASKS, rejectedExecutionHandler);
    }

    // -------------------------------------------------------------------------
    // Channel registration / deregistration
    // Called from AbstractSharedMemChannel.doRegister() / doDeregister()
    // which run on the event-loop thread, so no external locking is needed.
    // -------------------------------------------------------------------------

    /**
     * Adds a channel to the polling set.
     * Invoked by {@link AbstractSharedMemChannel#doRegister()}.
     */
    void addChannel(AbstractSharedMemChannel channel) {
        channels.add(channel);
        LOG.debug("Channel added to poll set: {}", channel);
    }

    /**
     * Removes a channel from the polling set.
     * Invoked by {@link AbstractSharedMemChannel#doDeregister()}.
     */
    void removeChannel(AbstractSharedMemChannel channel) {
        channels.remove(channel);
        LOG.debug("Channel removed from poll set: {}", channel);
    }

    // -------------------------------------------------------------------------
    // Main event loop
    // -------------------------------------------------------------------------

    @Override
    protected void run() {
        LOG.debug("SharedMemEventLoop started (busySpin={})", BUSY_SPIN);

        for (;;) {
            try {
                boolean hadData = pollChannels();

                // Run all pending tasks (submitted via execute() / schedule())
                runAllTasks();

                if (isShuttingDown() && confirmShutdown()) {
                    closeAllChannels();
                    break;
                }

                if (!hadData && !BUSY_SPIN) {
                    // Brief sleep to avoid burning 100% CPU when idle.
                    long ms  = IDLE_SLEEP_NS / 1_000_000;
                    int  ns  = (int) (IDLE_SLEEP_NS % 1_000_000);
                    if (ms > 0 || ns > 0) {
                        Thread.sleep(ms, ns);
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }

        LOG.debug("SharedMemEventLoop stopped");
    }

    // -------------------------------------------------------------------------
    // Polling helpers
    // -------------------------------------------------------------------------

    /**
     * Drains readable data from all registered channels.
     *
     * @return {@code true} if at least one channel had data
     */
    private boolean pollChannels() {
        boolean hadData = false;

        for (AbstractSharedMemChannel ch : channels) {
            if (!ch.isOpen()) {
                // Channel closed externally; remove lazily (ConcurrentHashMap is safe)
                channels.remove(ch);
                continue;
            }
            try {
                int n = 0;
                if (ch instanceof SharedMemServerChannel) {
                    SharedMemServerChannel server = (SharedMemServerChannel) ch;
                    if (server.hasReadableData()) {
                        n = server.doReadMessages();
                    }
                } else if (ch instanceof SharedMemChannel) {
                    SharedMemChannel data = (SharedMemChannel) ch;
                    if (data.hasReadableData()) {
                        n = data.doReadMessages();
                    }
                }
                if (n > 0) hadData = true;
            } catch (Throwable t) {
                LOG.warn("Exception while polling channel {}", ch, t);
                try {
                    ch.unsafe().close(ch.unsafe().voidPromise());
                } catch (Throwable ignore) { }
            }
        }
        return hadData;
    }

    private void closeAllChannels() {
        for (AbstractSharedMemChannel ch : channels) {
            try {
                ch.unsafe().close(ch.unsafe().voidPromise());
            } catch (Throwable t) {
                LOG.warn("Error closing channel {} during shutdown", ch, t);
            }
        }
        channels.clear();
    }

    private static void handleLoopException(Throwable t) {
        LOG.error("Unexpected error in SharedMemEventLoop", t);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // SingleThreadEventLoop hooks
    // -------------------------------------------------------------------------

    @Override
    protected void cleanup() {
        LOG.debug("SharedMemEventLoop cleanup");
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        // No selector to wake up; the loop self-polls.
        // Set a flag so the sleep in run() can be shortened in future if needed.
        wakenUp.set(true);
    }
}