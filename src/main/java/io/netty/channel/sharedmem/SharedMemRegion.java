package io.netty.channel.sharedmem;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a memory-mapped shared memory region used by the SharedMem protocol.
 * <p>
 * Internally backed by a memory-mapped file (POSIX-style shm emulation). The region
 * is divided into a fixed header area and a circular ring buffer for message passing.
 *
 * <pre>
 * Region Layout:
 * ┌──────────────────────────────────────────────────┐
 * │  HEADER (64 bytes)                               │
 * │  [0..3]   magic number (0xSHMM)                 │
 * │  [4..7]   version                               │
 * │  [8..11]  write index (producer)                │
 * │  [12..15] read  index (consumer)                │
 * │  [16..19] region capacity (bytes)               │
 * │  [20..63] reserved                              │
 * ├──────────────────────────────────────────────────┤
 * │  DATA (capacity bytes - HEADER_SIZE bytes)       │
 * │  Circular ring buffer of framed messages         │
 * │  Each frame: [4-byte length][payload bytes]      │
 * └──────────────────────────────────────────────────┘
 * </pre>
 */
public final class SharedMemRegion implements Closeable {

    public static final int HEADER_SIZE       = 64;
    public static final int MAGIC             = 0x53484D4D; // "SHMM"
    public static final int VERSION           = 1;
    public static final int FRAME_HEADER_SIZE = 4; // 4-byte message length prefix

    // Header field offsets
    private static final int OFF_MAGIC     = 0;
    private static final int OFF_VERSION   = 4;
    private static final int OFF_WRITE_IDX = 8;
    private static final int OFF_READ_IDX  = 12;
    private static final int OFF_CAPACITY  = 16;

    private static final String SHM_DIR = System.getProperty("sharedmem.dir",
            System.getProperty("java.io.tmpdir") + "/sharedmem");

    private final String regionName;
    private final int    totalSize;   // including header
    private final int    dataSize;    // ring buffer size = totalSize - HEADER_SIZE

    private RandomAccessFile  backingFile;
    private FileChannel       fileChannel;
    private MappedByteBuffer  mappedBuffer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Opens (or creates) a shared memory region.
     *
     * @param regionName the unique name of the region
     * @param capacity   total size in bytes (must be > HEADER_SIZE)
     * @throws IOException if the backing file cannot be created or mapped
     */
    public SharedMemRegion(String regionName, int capacity) throws IOException {
        if (capacity <= HEADER_SIZE) {
            throw new IllegalArgumentException("capacity must be > " + HEADER_SIZE);
        }
        this.regionName = regionName;
        this.totalSize  = capacity;
        this.dataSize   = capacity - HEADER_SIZE;

        Path shmDir = Paths.get(SHM_DIR);
        if (!Files.exists(shmDir)) {
            Files.createDirectories(shmDir);
        }

        Path shmFile = shmDir.resolve(regionName + ".shm");
        boolean isNew = !Files.exists(shmFile);

        backingFile  = new RandomAccessFile(shmFile.toFile(), "rw");
        backingFile.setLength(totalSize);
        fileChannel  = backingFile.getChannel();
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
        mappedBuffer.order(java.nio.ByteOrder.nativeOrder());

        if (isNew) {
            initHeader();
        } else {
            validateHeader();
        }
    }

    // -------------------------------------------------------------------------
    // Header helpers
    // -------------------------------------------------------------------------

    private void initHeader() {
        mappedBuffer.putInt(OFF_MAGIC,     MAGIC);
        mappedBuffer.putInt(OFF_VERSION,   VERSION);
        mappedBuffer.putInt(OFF_WRITE_IDX, 0);
        mappedBuffer.putInt(OFF_READ_IDX,  0);
        mappedBuffer.putInt(OFF_CAPACITY,  dataSize);
        mappedBuffer.force();
    }

    private void validateHeader() {
        int magic = mappedBuffer.getInt(OFF_MAGIC);
        if (magic != MAGIC) {
            throw new IllegalStateException(
                    String.format("Invalid SharedMem magic: expected 0x%X, got 0x%X", MAGIC, magic));
        }
    }

    private int getWriteIndex() { return mappedBuffer.getInt(OFF_WRITE_IDX); }
    private int getReadIndex()  { return mappedBuffer.getInt(OFF_READ_IDX);  }
    private void setWriteIndex(int idx) { mappedBuffer.putInt(OFF_WRITE_IDX, idx); }
    private void setReadIndex(int idx)  { mappedBuffer.putInt(OFF_READ_IDX,  idx); }

    /** Converts a ring offset to an absolute buffer position. */
    private int dataOffset(int ringIndex) {
        return HEADER_SIZE + (ringIndex % dataSize);
    }

    // -------------------------------------------------------------------------
    // Write path  (producer)
    // -------------------------------------------------------------------------

    /**
     * Writes a framed message into the ring buffer.
     *
     * @param src    source buffer (position to limit will be written)
     * @return {@code true} if the message was written; {@code false} if the ring is full
     */
    public boolean write(ByteBuffer src) {
        checkOpen();
        int msgLen   = src.remaining();
        int frameLen = FRAME_HEADER_SIZE + msgLen;

        int writeIdx = getWriteIndex();
        int readIdx  = getReadIndex();
        int used     = (writeIdx - readIdx + dataSize) % dataSize;
        int free     = dataSize - used - 1;

        if (frameLen > free) {
            return false; // ring full
        }

        // Write 4-byte length prefix
        writeRingInt(writeIdx, msgLen);
        writeIdx = advance(writeIdx, FRAME_HEADER_SIZE);

        // Write payload (handles wrap-around)
        int srcStart = src.position();
        int remaining = msgLen;
        while (remaining > 0) {
            int absPos  = dataOffset(writeIdx);
            int canWrite = Math.min(remaining, dataSize - (writeIdx % dataSize));
            for (int i = 0; i < canWrite; i++) {
                mappedBuffer.put(absPos + i, src.get(srcStart + (msgLen - remaining) + i));
            }
            remaining -= canWrite;
            writeIdx   = advance(writeIdx, canWrite);
        }

        setWriteIndex(writeIdx);
        mappedBuffer.force();
        return true;
    }

    private void writeRingInt(int ringIndex, int value) {
        byte[] bytes = new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>>  8),
            (byte)(value)
        };
        for (int i = 0; i < 4; i++) {
            mappedBuffer.put(dataOffset(ringIndex + i), bytes[i]);
        }
    }

    // -------------------------------------------------------------------------
    // Read path (consumer)
    // -------------------------------------------------------------------------

    /**
     * Reads the next framed message from the ring buffer into {@code dst}.
     *
     * @param dst destination buffer; must have enough remaining space
     * @return number of bytes read, or -1 if the ring is empty
     */
    public int read(ByteBuffer dst) {
        checkOpen();
        int writeIdx = getWriteIndex();
        int readIdx  = getReadIndex();

        if (readIdx == writeIdx) {
            return -1; // ring empty
        }

        // Read 4-byte length prefix
        int msgLen = readRingInt(readIdx);
        readIdx = advance(readIdx, FRAME_HEADER_SIZE);

        if (msgLen > dst.remaining()) {
            // Caller's buffer is too small — skip the message
            readIdx = advance(readIdx, msgLen);
            setReadIndex(readIdx);
            return 0;
        }

        // Read payload
        int dstStart  = dst.position();
        int remaining = msgLen;
        while (remaining > 0) {
            int absPos   = dataOffset(readIdx);
            int canRead  = Math.min(remaining, dataSize - (readIdx % dataSize));
            for (int i = 0; i < canRead; i++) {
                dst.put(dstStart + (msgLen - remaining) + i, mappedBuffer.get(absPos + i));
            }
            remaining -= canRead;
            readIdx    = advance(readIdx, canRead);
        }
        dst.position(dstStart + msgLen);
        dst.limit(dstStart + msgLen);

        setReadIndex(readIdx);
        return msgLen;
    }

    private int readRingInt(int ringIndex) {
        int b0 = mappedBuffer.get(dataOffset(ringIndex))     & 0xFF;
        int b1 = mappedBuffer.get(dataOffset(ringIndex + 1)) & 0xFF;
        int b2 = mappedBuffer.get(dataOffset(ringIndex + 2)) & 0xFF;
        int b3 = mappedBuffer.get(dataOffset(ringIndex + 3)) & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    /** Returns true if the ring buffer has at least one message ready to read. */
    public boolean isReadable() {
        return getReadIndex() != getWriteIndex();
    }

    // -------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------

    private int advance(int index, int by) {
        return (index + by) % dataSize;
    }

    public String getRegionName() { return regionName; }
    public int    getDataSize()   { return dataSize; }
    public int    getTotalSize()  { return totalSize; }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("SharedMemRegion [" + regionName + "] is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (fileChannel != null) fileChannel.close();
            if (backingFile != null) backingFile.close();
        }
    }

    @Override
    public String toString() {
        return "SharedMemRegion{name='" + regionName + "', totalSize=" + totalSize + '}';
    }
}
