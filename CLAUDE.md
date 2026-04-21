# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Build
mvn clean package

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=SharedMemRegionTest

# Run examples (two terminals)
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoServer
mvn exec:java -Dexec.mainClass=io.netty.channel.sharedmem.example.EchoClient
```

Override the shared-memory directory: `-Dsharedmem.dir=/your/path` (default: `$TMPDIR/sharedmem/`).

## Architecture

A Netty transport extension for same-host IPC via memory-mapped ring buffers. All classes live in `io.netty.channel.sharedmem` intentionally, to integrate with Netty internals. No JNI — pure Java via `MappedByteBuffer`.

### Connection Lifecycle

```
SERVER                          CLIENT
doBind()
  └─ creates CQ region only     doConnect()
     (passive, no data region)    1. creates dedicated data region (client-owned)
                                  2. opens server's CQ region
pollAccept() ◄── token ────────   3. writes SharedMemConnectionToken to CQ
  └─ opens client's data region
  └─ creates SharedMemChannel child
  └─ fires channelRead(child)
```

### Key Classes

| Class | Role |
|---|---|
| `SharedMemRegion` | mmap'd ring buffer; **not thread-safe** — one instance per event-loop thread |
| `AbstractSharedMemChannel` | Base lifecycle, address fields, `readFromRegion`/`writeToRegion` helpers |
| `SharedMemChannel` | Full-duplex data channel; `pollRead()` called each event-loop tick |
| `SharedMemServerChannel` | Binds CQ region, runs `pollAccept()` to emit child channels |
| `SharedMemEventLoop` | Single-threaded polling loop; sleeps `pollIntervalNs` between ticks |
| `SharedMemAddress` | `SocketAddress` subclass — name + port |
| `SharedMemConnectionToken` | Fixed 128-byte serialized handshake token written to CQ |

### Ring Buffer Format

```
[0..3]  magic 0x53484D4D ("SHMM")
[4..7]  version = 1
[8..15] write cursor (monotonically increasing long, producer)
[16..23] read cursor (monotonically increasing long, consumer)
[24..27] data capacity
[28..31] flags (bit0=READY, bit1=CLOSED)
[32..]  circular data buffer
```

Cursors are monotonically increasing; `dataIndex = cursor % capacity`. Values use JVM native byte order.

### Region Files

- Stored as `.shm` files under `sharedmem.dir`
- CQ region name = `serverRegionName + ".cq"` (constant `SharedMemServerChannel.CQ_SUFFIX`)
- Client data regions use format `<serverRegion>_<clientPort>_<8-char-uuid>`
- When opening an existing region (`create=false`), capacity is read from the file header, not the constructor arg

### Design Constraints

- `SharedMemRegion` is not thread-safe; each instance must be owned by one event-loop thread
- `pollIntervalNs = 0` gives pure busy-spin (minimum latency, 100% CPU)
- The server never creates data regions — only the client does
- `LoggingHandler` in `src/main/java/io/netty/handler/logging/` is a local copy, not from this project

