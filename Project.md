

## SharedMem Netty Transport — Project Summary for Claude Code

### What this is
A custom Netty transport extension for a hardware protocol called **SharedMem** (an enhanced remote file system protocol). It plugs into the standard Netty bootstrap API exactly like `NioEventLoopGroup` / `NioServerSocketChannel`, but communicates via **memory-mapped shared-memory ring buffers** instead of TCP sockets.

### Package
`io.netty.channel.sharedmem` — all classes live here intentionally inside the `io.netty` namespace to integrate naturally with Netty internals.

### Correct connection lifecycle
```
SERVER                                  CLIENT
──────                                  ──────
doBind()
  └─ creates CQ region only            doConnect()
     (passive listener, no data          1. generate unique data-region name
      region, no child channels)         2. create data region (client owns this)
                                         3. open server's CQ region
pollAccept() ◄──── token ────────────   4. write SharedMemConnectionToken to CQ
  └─ reads token from CQ               5. fire channelActive
  └─ opens client's data region
  └─ creates SharedMemChannel child
  └─ fires channelRead(child)
```

### Backing file locations
Regions are stored as `.shm` files under `$TMPDIR/sharedmem/` by default. Override with system property `-Dsharedmem.dir=/your/path`. CQ region name = `regionName + ".cq"`.


### Dependencies
Netty `4.1.108.Final` — `netty-common`, `netty-buffer`, `netty-transport`, `netty-handler`, `netty-codec`. Java 11+. No JNI, no native code.

### Known design constraints
- `SharedMemRegion` is **not thread-safe** — each instance must be accessed from one thread (the owning event-loop thread).
- When opening an existing region (`create=false`), capacity is **read from the file header**, not from the constructor argument.
- `pollIntervalNs = 0` gives pure busy-spin (lowest latency, 100% CPU on one core).
- `LoggingHandler` (from `netty-handler`) auto-generates the `[id: 0x..., L:sharedmem://...] ACTIVE` log lines if added to the pipeline — it is not from this project.