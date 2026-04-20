package io.netty.channel.sharedmem;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * A {@link MultithreadEventLoopGroup} that creates {@link SharedMemEventLoop} instances.
 *
 * <p>Mirrors the constructor signatures of {@code NioEventLoopGroup} for drop-in
 * familiarity. The number of threads defaults to 2× the number of available
 * processors if not specified.
 *
 * <h3>Example</h3>
 * <pre>{@code
 * // Server bootstrap
 * SharedMemEventLoopGroup bossGroup   = new SharedMemEventLoopGroup(1);
 * SharedMemEventLoopGroup workerGroup = new SharedMemEventLoopGroup();
 *
 * // Client bootstrap
 * SharedMemEventLoopGroup group = new SharedMemEventLoopGroup();
 * }</pre>
 */
public class SharedMemEventLoopGroup extends MultithreadEventLoopGroup {

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a new instance with the default number of threads (2 × cpu count).
     */
    public SharedMemEventLoopGroup() {
        this(0);
    }

    /**
     * Creates a new instance with the specified number of threads.
     *
     * @param nThreads number of event-loop threads; 0 means use the default
     */
    public SharedMemEventLoopGroup(int nThreads) {
        this(nThreads, (ThreadFactory) null);
    }

    /**
     * Creates a new instance.
     *
     * @param nThreads      number of event-loop threads
     * @param threadFactory thread factory, or {@code null} for the default
     */
    public SharedMemEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, DefaultSelectStrategyFactory.INSTANCE);
    }

    /**
     * Creates a new instance.
     *
     * @param nThreads              number of event-loop threads
     * @param threadFactory         thread factory
     * @param selectStrategyFactory select strategy (unused by SharedMem but kept for API parity)
     */
    public SharedMemEventLoopGroup(int nThreads,
                                   ThreadFactory threadFactory,
                                   SelectStrategyFactory selectStrategyFactory) {
        super(nThreads,
              threadFactory == null
                  ? new DefaultThreadFactory(SharedMemEventLoopGroup.class)
                  : threadFactory,
              selectStrategyFactory);
    }

    /**
     * Creates a new instance.
     *
     * @param nThreads                 number of event-loop threads
     * @param executor                 executor to use for running event loops
     * @param selectStrategyFactory    select strategy factory
     * @param rejectedExecutionHandler handler for rejected tasks
     */
    public SharedMemEventLoopGroup(int nThreads,
                                   Executor executor,
                                   SelectStrategyFactory selectStrategyFactory,
                                   RejectedExecutionHandler rejectedExecutionHandler) {
        super(nThreads, executor, selectStrategyFactory, rejectedExecutionHandler);
    }

    // -------------------------------------------------------------------------
    // MultithreadEventLoopGroup factory method
    // -------------------------------------------------------------------------

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        SelectStrategyFactory selectStrategyFactory = args.length > 0
                ? (SelectStrategyFactory) args[0]
                : DefaultSelectStrategyFactory.INSTANCE;

        RejectedExecutionHandler rejectedExecutionHandler = args.length > 1
                ? (RejectedExecutionHandler) args[1]
                : RejectedExecutionHandlers.reject();

        return new SharedMemEventLoop(
                this,
                executor,
                selectStrategyFactory,
                rejectedExecutionHandler
        );
    }
}
