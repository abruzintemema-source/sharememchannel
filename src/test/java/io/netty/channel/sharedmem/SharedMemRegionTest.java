package io.netty.channel.sharedmem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SharedMemRegion}.
 */
class SharedMemRegionTest {

    @TempDir
    Path tempDir;

    private SharedMemRegion region;

    @BeforeEach
    void setUp() throws IOException {
        // Point shm dir at JUnit's temp directory
        System.setProperty("sharedmem.dir", tempDir.toAbsolutePath().toString());
        region = new SharedMemRegion("test_region", 64 * 1024); // 64 KB
    }

    @AfterEach
    void tearDown() throws IOException {
        if (region != null) region.close();
    }

    @Test
    void testWriteAndRead() throws IOException {
        byte[] data    = "Hello, SharedMem!".getBytes();
        ByteBuffer src = ByteBuffer.wrap(data);
        ByteBuffer dst = ByteBuffer.allocate(1024);

        assertTrue(region.write(src), "write should succeed");

        dst.clear();
        int n = region.read(dst);

        assertEquals(data.length, n, "read byte count mismatch");
        dst.position(0);
        byte[] result = new byte[n];
        dst.get(result);
        assertArrayEquals(data, result);
    }

    @Test
    void testMultipleMessages() throws IOException {
        String[] messages = {"alpha", "beta", "gamma", "delta"};

        for (String m : messages) {
            ByteBuffer src = ByteBuffer.wrap(m.getBytes());
            assertTrue(region.write(src));
        }

        for (String expected : messages) {
            ByteBuffer dst = ByteBuffer.allocate(256);
            int n = region.read(dst);
            assertTrue(n > 0);
            dst.position(0);
            byte[] bytes = new byte[n];
            dst.get(bytes);
            assertEquals(expected, new String(bytes));
        }
    }

    @Test
    void testEmptyReadReturnsNegativeOne() throws IOException {
        ByteBuffer dst = ByteBuffer.allocate(256);
        int n = region.read(dst);
        assertEquals(-1, n, "empty ring should return -1");
    }

    @Test
    void testIsReadable() throws IOException {
        assertFalse(region.isReadable(), "should not be readable when empty");

        ByteBuffer src = ByteBuffer.wrap("ping".getBytes());
        region.write(src);

        assertTrue(region.isReadable(), "should be readable after write");

        ByteBuffer dst = ByteBuffer.allocate(256);
        region.read(dst);

        assertFalse(region.isReadable(), "should not be readable after full drain");
    }

    @Test
    void testWrapAround() throws IOException {
        // Fill ring close to capacity, then drain, then write again to exercise wrap-around
        int dataSize = region.getDataSize();
        int msgLen   = dataSize / 4 - SharedMemRegion.FRAME_HEADER_SIZE;
        byte[] payload = new byte[msgLen];
        java.util.Arrays.fill(payload, (byte) 0x42);

        for (int i = 0; i < 3; i++) {
            region.write(ByteBuffer.wrap(payload));
        }
        for (int i = 0; i < 3; i++) {
            ByteBuffer dst = ByteBuffer.allocate(msgLen + 8);
            region.read(dst);
        }
        // Ring head is now near mid-buffer — next write should wrap around
        for (int i = 0; i < 3; i++) {
            assertTrue(region.write(ByteBuffer.wrap(payload)), "wrap-around write " + i);
        }
        for (int i = 0; i < 3; i++) {
            ByteBuffer dst = ByteBuffer.allocate(msgLen + 8);
            int n = region.read(dst);
            assertEquals(msgLen, n, "wrap-around read " + i);
        }
    }

    @Test
    void testClosedRegionThrows() throws IOException {
        region.close();
        assertThrows(IllegalStateException.class, () ->
                region.write(ByteBuffer.wrap("x".getBytes())));
    }
}
