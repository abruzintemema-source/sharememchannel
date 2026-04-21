# netty-transport-sharedmem

A Netty transport extension that adds a **SharedMem** protocol — a zero-copy, shared-memory ring-buffer channel for ultra-low-latency same-host (or NUMA-local) communication. Plugs into the standard Netty bootstrap API exactly like `NioEventLoopGroup` / `NioServerSocketChannel`, but communicates via memory-mapped ring buffers instead of TCP sockets.

No JNI, no native code — pure Java via `MappedByteBuffer`. Java 11+.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      User Application                            │
│  ServerBootstrap / Bootstrap                                     │
└─────────────────────────┬────────────────────────────────────────┘
                          │
        ┌─────────────────▼──────────────────┐
        │     SharedMemEventLoopGroup        │
        │  (MultithreadEventLoopGroup)       │
        └─────────────────┬──────────────────┘
                          │  1..N
        ┌─────────────────▼──────────────────┐
        │       SharedMemEventLoop           │  ← polls registered channels
        │  (SingleThreadEventLoop)           │    every ~100 µs (configurable)
        └─────────────────┬──────────────────┘
                          │ registers
          ┌───────────────┴────────────────────────┐
          │                                        │
┌─────────▼──────────┐              ┌──────────────▼────────────┐
│ SharedMemServer    │              │    SharedMemChannel        │
│    Channel         │  accepts →   │  (full-duplex data ch.)   │
│ (listen / accept)  │              │                            │
└────────────────────┘              └──────────┬─────────────────┘
                                              │ owns
                                   ┌──────────▼──────────────┐
                                   │  txRegion  │  rxRegion   │
                                   │  (mmap'd ring buffers)   │
                                   └──────────────────────────┘
```

---

## Bidirectional Design (Two-Region Protocol)

Each connection uses **two independent ring-buffer regions** — one per direction — so client and server never contend on the same ring:

| Region file | Creator | Writer | Reader |
|---|---|---|---|
| `<uuid>` | Client | Client | Server |
| `<uuid>.s2c` | Server | Server | Client |

From each channel's perspective: `txRegion` is the ring it writes into; `rxRegion` is the ring it reads from.

---

## Connection Lifecycle

```
SERVER                                   CLIENT
──────                                   ──────
doBind()
  └─ creates CQ region only             doConnect()
     (<name>.cq backing file)             1. generate unique region name (uuid)
                                          2. create txRegion  (<uuid>.shm)
                                          3. open server CQ, write token
pollAccept() ◄──── token ─────────────   4. poll until server creates rxRegion
  └─ opens client's txRegion as rxRegion     (<uuid>.s2c.shm)
  └─ creates its own txRegion            5. open rxRegion (<uuid>.s2c)
       (<uuid>.s2c backing file)         6. fire channelActive → pipeline ready
  └─ creates SharedMemChannel child
  └─ fires channelRead(child) → pipeline
```

The client's `doConnect` blocks briefly (≤5 s timeout, polling every 1 ms) until the server creates the `.s2c` file. In practice this completes within one boss poll interval (~100 µs).

---

## Classes

| Class | Role |
|---|---|
| `SharedMemAddress` | `SocketAddress` subclass — identifies an endpoint by region name + node ID |
| `SharedMemRegion` | Low-level mmap'd ring buffer (raw byte producer/consumer) |
| `SharedMemChannelConfig` | Per-channel options (`regionSize`, `pollIntervalUs`, …) |
| `SharedMemChannelOption` | Static `ChannelOption` constants |
| `AbstractSharedMemChannel` | Base channel — lifecycle, `txRegion`/`rxRegion`, `readFromRegion`, `writeToRegion` |
| `SharedMemChannel` | Full-duplex data channel; `pollRead()` called each event-loop tick |
| `SharedMemServerChannel` | Server channel — scans CQ region for new clients via `pollAccept()` |
| `SharedMemEventLoop` | Single-threaded polling event loop |
| `SharedMemEventLoopGroup` | Multi-threaded group of event loops |
| `SharedMemChannelFactory` | `ChannelFactory<SharedMemChannel>` |
| `SharedMemServerChannelFactory` | `ChannelFactory<SharedMemServerChannel>` |
| `SharedMemConnectionToken` | Fixed 128-byte handshake token written by client into the CQ region |

---

## Ring Buffer Layout

Each `.shm` backing file has a fixed 32-byte header followed by the circular data buffer:

```
Offset  Size  Field
──────  ────  ─────────────────────────────────────────────
 0       4    Magic number  (0x53484D4D = ASCII "SHMM")
 4       4    Version       (1)
 8       8    Write cursor  (monotonically increasing long, producer)
16       8    Read  cursor  (monotonically increasing long, consumer)
24       4    Capacity      (size of the circular data area in bytes)
28       4    Flags         (bit 0 = READY, bit 1 = CLOSED)
32     [cap]  Circular data buffer (raw bytes, no internal framing)
```

Cursors are monotonically increasing; `dataIndex = cursor % capacity`. Values use the JVM's native byte order. Message framing (e.g. 4-byte length prefix) is handled by pipeline handlers (`LengthFieldPrepender` / `LengthFieldBasedFrameDecoder`), not by the ring itself.

---

## Backing File Locations

| File | Purpose |
|---|---|
| `<sharedmem.dir>/<name>.shm` | Any ring-buffer region |
| `<name>.cq.shm` | Server connection-queue region (created at bind) |
| `<name>.s2c.shm` | Server→client data region (created per accepted connection) |

Default directory: `$TMPDIR/sharedmem/`. Override: `-Dsharedmem.dir=/your/path`.

---

## Quick Start

### Server
```java
SharedMemEventLoopGroup boss   = new SharedMemEventLoopGroup(1);
SharedMemEventLoopGroup worker = new SharedMemEventLoopGroup();

new ServerBootstrap()
    .group(boss, worker)
    .channel(SharedMemServerChannel.class)
    .childOption(SharedMemChannelOption.REGION_SIZE, 8 * 1024 * 1024)
    .childHandler(new ChannelInitializer<SharedMemChannel>() {
        protected void initChannel(SharedMemChannel ch) {
            ch.pipeline().addLast(
                new LengthFieldBasedFrameDecoder(8_000_000, 0, 4, 0, 4),
                new LengthFieldPrepender(4),
                new MyServerHandler()
            );
        }
    })
    .bind(new SharedMemAddress("my_server"))
    .sync();
```

### Client
```java
new Bootstrap()
    .group(new SharedMemEventLoopGroup())
    .channel(SharedMemChannel.class)
    .option(SharedMemChannelOption.REGION_SIZE, 8 * 1024 * 1024)
    .option(SharedMemChannelOption.POLL_INTERVAL_US, 50)
    .handler(new ChannelInitializer<SharedMemChannel>() {
        protected void initChannel(SharedMemChannel ch) {
            ch.pipeline().addLast(
                new LengthFieldBasedFrameDecoder(8_000_000, 0, 4, 0, 4),
                new LengthFieldPrepender(4),
                new MyClientHandler()
            );
        }
    })
    .connect(new SharedMemAddress("my_server"))
    .sync();
```

---

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `REGION_SIZE` | `int` | 4 MB | Shared memory ring size in bytes (applied to both tx and rx rings) |
| `POLL_INTERVAL_US` | `int` | 100 µs | Sleep between polls when ring is empty |
| `MAX_MESSAGES_PER_READ` | `int` | 16 | Max read iterations per event-loop tick |
| `DIRECT_BUFFER` | `boolean` | `true` | Use direct (off-heap) ByteBuffers |

### JVM System Properties

| Property | Default | Description |
|---|---|---|
| `sharedmem.dir` | `$TMPDIR/sharedmem` | Directory for `.shm` backing files |
| `sharedmem.idleSleepNs` | `100000` | Idle sleep in nanoseconds |
| `sharedmem.busySpin` | `false` | Pure spin mode (min latency, max CPU) |

---

## Build

```bash
mvn clean package
```

---

## Running the Examples

```bash
# Terminal 1 — server (echoes back every message with " over" appended)
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoServer

# Terminal 2 — client
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoClient
```

Expected output:
```
[EchoServer]  Received: Hello, SharedMem!
[EchoClient] Sent:     Hello, SharedMem!
[EchoClient] Received: Hello, SharedMem! over 2026-04-21 09:44:38.995
```

The server appends `" over <yyyy-MM-dd HH:mm:ss.SSS>"` — the literal word "over" plus the server-side timestamp at millisecond precision.

---

## Design Constraints

- `SharedMemRegion` is **not thread-safe** — each instance must be accessed from one thread (its owning event-loop thread).
- When opening an existing region (`create=false`), capacity is read from the file header; the constructor argument is ignored.
- `POLL_INTERVAL_US = 0` / `-Dsharedmem.busySpin=true` gives pure busy-spin (lowest latency, 100% CPU on one core).
- The server never creates the client→server data region — only the client does. The server opens it read-write after reading the connection token.
- `LoggingHandler` under `io.netty.handler.logging` in this repo is a local copy, not from netty-handler.
