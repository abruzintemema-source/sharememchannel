package io.netty.channel.sharedmem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Low-level shared-memory ring-buffer region.
 *
 * <p>Backed by a {@link MappedByteBuffer} so it works cross-process on any OS
 * without JNI for the initial implementation.  A native-backed version (e.g.
 * using {@code shm_open} / {@code mmap} via JNI) can be swapped in by replacing
 * only this class while keeping the rest of the API unchanged.
 *
 * <h3>Region layout (bytes)</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ─────────────────────────────────────────────
 *  0       4    Magic number  (0x53484D4D = ASCII "SHMM")
 *  4       4    Version       (currently 1)
 *  8       8    Write cursor  (monotonically increasing long, producer side)
 * 16       8    Read  cursor  (monotonically increasing long, consumer side)
 * 24       4    Capacity      (size of the circular data area in bytes)
 * 28       4    Flags         (bit 0 = READY, bit 1 = CLOSED)
 * 32      [cap] Circular data buffer
 * </pre>
 *
 * All multi-byte values are stored in the JVM's native byte order
 * ({@link java.nio.ByteOrder#nativeOrder()}).  Cursors are monotonically
 * increasing so modular arithmetic automatically handles wrap-around:
 * {@code dataIndex = cursor % capacity}.
 *
 * <h3>Thread safety</h3>
 * A single {@link SharedMemRegion} instance is <em>not</em> thread-safe.
 * If multiple threads need to read or write concurrently, external
 * synchronisation is required.  In the Netty integration each region
 * is accessed exclusively from its owning event-loop thread.
 *  *
 * <h3>Memory visibility assumptions (IPC)</h3>
 * This transport assumes a single producer / single consumer pattern where:
 * <ul>
 *   <li>The producer writes payload bytes first and then publishes {@code writeCursor}.</li>
 *   <li>The consumer reads {@code writeCursor}, then reads payload bytes, then publishes {@code readCursor}.</li>
 * </ul>
 * {@link VarHandle#releaseFence()} and {@link VarHandle#acquireFence()} are used around cursor publication/consumption
 * so cursor updates act as publish points for ring data visibility across threads/processes that map the same region.
 * Durability (persisting dirty pages to storage) is intentionally decoupled from visibility.
 */
public final class SharedMemRegion implements Closeable {

    // ─────────────────────────────────────────────────────────────────────────
    // Public constants
    // ─────────────────────────────────────────────────────────────────────────

    /** Magic number stored in the first 4 bytes of every region: {@code "SHMM"}. */
    public static final int  MAGIC       = 0x53484D4D;

    /** Current wire-format version. */
    public static final int  VERSION     = 1;

    /** Size in bytes of the fixed region header. */
    public static final int  HEADER_SIZE = 32;

    /** Flag bit indicating the region is initialised and ready to use. */
    public static final int  FLAG_READY  = 0x01;

    /** Flag bit indicating the region has been closed by its creator. */
    public static final int  FLAG_CLOSED = 0x02;

    // ─────────────────────────────────────────────────────────────────────────
    // Private header-field offsets
    // ─────────────────────────────────────────────────────────────────────────

    private static final int OFF_MAGIC      =  0;
    private static final int OFF_VERSION    =  4;
    private static final int OFF_WRITE_CUR  =  8;
    private static final int OFF_READ_CUR   = 16;
    private static final int OFF_CAPACITY   = 24;
    private static final int OFF_FLAGS      = 28;

    // ─────────────────────────────────────────────────────────────────────────
    // Instance state
    // ─────────────────────────────────────────────────────────────────────────

    private final String          regionName;
    private int                   capacity; // set from header when opening existing region
    private final FileChannel     fileChannel;
    private final MappedByteBuffer mapping;
    private final boolean         forceOnClose;
    private final int             flushEveryOps;
    private int                   operationCountSinceFlush;
    private final AtomicBoolean   closed = new AtomicBoolean(false);

    // ─────────────────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Opens or creates a shared-memory region.
     *
     * @param regionName name of the region; used to derive the backing file path
     * @param capacity   size of the circular data buffer in bytes; must be &gt; 0.
     *                   Ignored (header value used instead) when {@code create} is {@code false}.
     * @param create     {@code true} → create (or truncate) the backing file and write the header;
     *                   {@code false} → open an existing region and validate the header
     * 
     * <p>Flush policy is controlled via system properties:
     * <ul>
     *   <li>{@code sharedmem.force.on.close} (default: {@code true}) – force dirty pages on close.</li>
     *   <li>{@code sharedmem.flush.every.ops} (default: {@code 0}) – force every N read/write operations.</li>
     * </ul>
     * Per-operation force is disabled by default because it is intended only for diagnostics/durability scenarios,
     * not normal transport throughput.
     * @throws IOException              if the backing file cannot be opened or mapped
     * @throws IllegalArgumentException if {@code capacity} is &le; 0
     * @throws IllegalStateException    if {@code create=false} and the header magic is wrong
     */
    public SharedMemRegion(String regionName, int capacity, boolean create) throws IOException {
        this(regionName, capacity, create,
                Boolean.parseBoolean(System.getProperty("sharedmem.force.on.close", "true")),
                Integer.getInteger("sharedmem.flush.every.ops", 0));
    }

    SharedMemRegion(String regionName,
                    int capacity,
                    boolean create,
                    boolean forceOnClose,
                    int flushEveryOps) throws IOException {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got: " + capacity);
        }
        if (flushEveryOps < 0) {
            throw new IllegalArgumentException("flushEveryOps must be >= 0, got: " + flushEveryOps);
        }
        this.regionName = regionName;
        this.capacity   = capacity;
        this.forceOnClose = forceOnClose;
        this.flushEveryOps = flushEveryOps;

        Path   backingFile = resolveBackingFile(regionName);
        long   totalSize   = (long) HEADER_SIZE + capacity;

        if (create) {
            Files.createDirectories(backingFile.getParent());
            this.fileChannel = FileChannel.open(backingFile,
                    EnumSet.of(StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING));
            // Extend the file to the required size by writing a single byte at the end.
            this.fileChannel.position(totalSize - 1);
            this.fileChannel.write(ByteBuffer.wrap(new byte[]{ 0 }));
            this.mapping = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
            initHeader();
        } else {
            this.fileChannel = FileChannel.open(backingFile,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            long existingSize = this.fileChannel.size();
            this.mapping = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, existingSize);
            validateHeader();
            // Read the actual capacity from the header — the constructor arg is only a hint
            // when opening an existing region. The file's header value is authoritative.
            this.capacity = this.mapping.getInt(OFF_CAPACITY);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API – read / write
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Writes up to {@code length} bytes from {@code src} (starting at {@code offset})
     * into the circular buffer.
     *
     * <p>If the ring is too full to accept all {@code length} bytes, only the bytes
     * that fit are written.  The caller is responsible for retrying any unwritten remainder.
     *
     * @param src    source byte array
     * @param offset starting index in {@code src}
     * @param length number of bytes to attempt to write
     * @return number of bytes actually written ({@code 0} if the ring is full)
     * @throws IllegalStateException if the region has been closed
     */
    public int write(byte[] src, int offset, int length) {
        ensureOpen();
        long writeCursor = readLong(OFF_WRITE_CUR);
        long readCursor  = readLong(OFF_READ_CUR);
        int  used        = (int) (writeCursor - readCursor);
        int  available   = capacity - used;
        int  toWrite     = Math.min(length, available);
        if (toWrite <= 0) return 0;

        int startPos = (int) (writeCursor % capacity);
        for (int i = 0; i < toWrite; i++) {
            int index = (startPos + i) % capacity;
            mapping.put(HEADER_SIZE + index, src[offset + i]);
        }
        writeLong(OFF_WRITE_CUR, writeCursor + toWrite);
        mapping.force();
        return toWrite;
    }

    /**
     * Reads up to {@code maxLength} bytes from the circular buffer into {@code dst}
     * (starting at {@code offset}).
     *
     * @param dst       destination byte array
     * @param offset    starting index in {@code dst}
     * @param maxLength maximum number of bytes to read
     * @return number of bytes actually read ({@code 0} if the ring is empty)
     * @throws IllegalStateException if the region has been closed
     */
    public int read(byte[] dst, int offset, int maxLength) {
        ensureOpen();
        long writeCursor = readLong(OFF_WRITE_CUR);
        long readCursor  = readLong(OFF_READ_CUR);
        int  available   = (int) (writeCursor - readCursor);
        int  toRead      = Math.min(maxLength, available);
        if (toRead <= 0) return 0;
        
        VarHandle.acquireFence();
        int startPos = (int) (readCursor % capacity);
        for (int i = 0; i < toRead; i++) {
            int index = (startPos + i) % capacity;
            dst[offset + i] = mapping.get(HEADER_SIZE + index);
        }
        VarHandle.releaseFence();
        writeLong(OFF_READ_CUR, readCursor + toRead);
        maybeFlush();
        return toRead;
    }

    public ByteBuffer readAsByteBuffer(int length) {
    ensureOpen();

    long writeCursor = readLong(OFF_WRITE_CUR);
    long readCursor  = readLong(OFF_READ_CUR);

    int available = (int) (writeCursor - readCursor);
    int toRead    = Math.min(length, available);

    if (toRead <= 0) {
        return ByteBuffer.allocate(0);
    }

    int startPos = (int) (readCursor % capacity);

    // ⚠️ Handle wrap-around
    if (startPos + toRead <= capacity) {
        // ✅ contiguous → true zero-copy slice
        ByteBuffer dup = mapping.duplicate();
        dup.position(HEADER_SIZE + startPos);
        dup.limit(HEADER_SIZE + startPos + toRead);

        ByteBuffer slice = dup.slice();

        writeLong(OFF_READ_CUR, readCursor + toRead);
        return slice;
    } else {
        // ❌ wrap-around → fallback (temporary copy)
        byte[] tmp = new byte[toRead];
        read(tmp, 0, toRead);
        return ByteBuffer.wrap(tmp);
    }
}
    // ─────────────────────────────────────────────────────────────────────────
    // Public API – introspection
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the number of bytes currently available to read from the ring buffer.
     */
    public int readableBytes() {
        ensureOpen();
        return (int) (readLong(OFF_WRITE_CUR) - readLong(OFF_READ_CUR));
    }

    /**
     * Returns the number of bytes that can still be written to the ring buffer
     * without blocking.
     */
    public int writableBytes() {
        long writeCursor = readLong(OFF_WRITE_CUR);
        long readCursor  = readLong(OFF_READ_CUR);

        int used = (int)(writeCursor - readCursor);
        return capacity - used;
    }
    /**
     * Returns the name of this shared-memory region.
     */
    public String getRegionName() {
        return regionName;
    }

    /**
     * Returns the capacity (size of the data area) of the ring buffer in bytes.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Returns {@code true} if this region has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Closeable
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Marks the region as closed (sets {@link #FLAG_CLOSED} in the header),
     * * optionally forces pending writes to the backing file, and releases the file channel.
     *
     * <p>Idempotent – calling {@code close()} multiple times is safe.
     *
     * @throws IOException if an I/O error occurs while releasing resources
     */
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            writeInt(OFF_FLAGS, readInt(OFF_FLAGS) | FLAG_CLOSED);
            if (forceOnClose) {
                mapping.force();
            }
            fileChannel.close();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers – header I/O
    // ─────────────────────────────────────────────────────────────────────────

    private void initHeader() {
        writeInt(OFF_MAGIC,    MAGIC);
        writeInt(OFF_VERSION,  VERSION);
        writeLong(OFF_WRITE_CUR, 0L);
        writeLong(OFF_READ_CUR,  0L);
        writeInt(OFF_CAPACITY, capacity);
        writeInt(OFF_FLAGS,    FLAG_READY);
        mapping.force();
    }

    private void validateHeader() {
        int magic = readInt(OFF_MAGIC);
        if (magic != MAGIC) {
            throw new IllegalStateException(String.format(
                    "Invalid SharedMem region '%s': expected magic 0x%08X, found 0x%08X",
                    regionName, MAGIC, magic));
        }
        int version = readInt(OFF_VERSION);
        if (version != VERSION) {
            throw new IllegalStateException(String.format(
                    "Unsupported SharedMem region version: %d (supported: %d)", version, VERSION));
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("SharedMemRegion '" + regionName + "' is already closed");
        }
    }
    private void maybeFlush() {
        if (flushEveryOps <= 0) {
            return;
        }
        operationCountSinceFlush++;
        if (operationCountSinceFlush >= flushEveryOps) {
            mapping.force();
            operationCountSinceFlush = 0;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers – MappedByteBuffer accessors
    // ─────────────────────────────────────────────────────────────────────────

    private int readInt(int offset) {
        return mapping.getInt(offset);
    }

    private void writeInt(int offset, int value) {
        mapping.putInt(offset, value);
    }

    private long readLong(int offset) {
        return mapping.getLong(offset);
    }

    private void writeLong(int offset, long value) {
        mapping.putLong(offset, value);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Backing-file path resolution
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Resolves the file-system path for the backing file of a named region.
     *
     * <p>The directory can be overridden via the {@code sharedmem.dir} system property.
     * Defaults to {@code $TMPDIR/sharedmem/}.
     *
     * @param regionName region identifier
     * @return absolute path to the {@code .shm} file
     */
    static Path resolveBackingFile(String regionName) {
        String baseDir = System.getProperty(
                "sharedmem.dir",
                System.getProperty("java.io.tmpdir") + "/sharedmem");
        return Paths.get(baseDir, regionName + ".shm");
    }
}
