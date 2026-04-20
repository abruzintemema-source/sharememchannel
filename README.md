# netty-transport-sharedmem

A Netty transport extension that adds a **SharedMem** protocol — a zero-copy, shared-memory ring-buffer channel for ultra-low-latency same-host (or NUMA-local) communication.

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
                                   │    SharedMemRegion (×2)  │
                                   │  tx ring  │  rx ring     │
                                   │  (mmap'd circular buf)   │
                                   └──────────────────────────┘
```

---

## Classes

| Class | Role |
|---|---|
| `SharedMemAddress` | `SocketAddress` subclass — identifies a region by name + node ID |
| `SharedMemRegion` | Low-level mmap'd ring buffer (producer/consumer framing) |
| `SharedMemChannelConfig` | Per-channel options (`regionSize`, `pollIntervalUs`, …) |
| `SharedMemChannelOption` | Static `ChannelOption` constants |
| `AbstractSharedMemChannel` | Base channel (lifecycle, address, helpers) |
| `SharedMemChannel` | Full-duplex data channel |
| `SharedMemServerChannel` | Server channel — scans shm dir for new clients |
| `SharedMemEventLoop` | Single-threaded polling event loop |
| `SharedMemEventLoopGroup` | Multi-threaded group of event loops |
| `SharedMemChannelFactory` | `ChannelFactory<SharedMemChannel>` |
| `SharedMemServerChannelFactory` | `ChannelFactory<SharedMemServerChannel>` |

---

## Ring Buffer Layout

```
┌──────────────────────────────────────────────────┐
│  HEADER (64 bytes)                               │
│  [0..3]   magic   = 0x53484D4D ("SHMM")         │
│  [4..7]   version = 1                            │
│  [8..11]  write index (producer)                 │
│  [12..15] read  index (consumer)                 │
│  [16..19] data capacity                          │
│  [20..63] reserved                               │
├──────────────────────────────────────────────────┤
│  DATA (capacity − 64 bytes)                      │
│  Circular ring — each frame:                     │
│    [4-byte big-endian length][payload bytes]     │
└──────────────────────────────────────────────────┘
```

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
    .connect(
        new SharedMemAddress("my_server"),
        new SharedMemAddress("my_client")
    )
    .sync();
```

---

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `REGION_SIZE` | `int` | 4 MB | Shared memory ring size in bytes |
| `POLL_INTERVAL_US` | `int` | 100 µs | Sleep between polls when ring is empty |
| `MAX_MESSAGES_PER_READ` | `int` | 16 | Max messages drained per event-loop tick |
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
# Terminal 1 — server
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoServer

# Terminal 2 — client
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoClient
```
