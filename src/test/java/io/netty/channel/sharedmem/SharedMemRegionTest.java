package io.netty.channel.sharedmem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

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
        System.setProperty("sharedmem.dir", tempDir.toAbsolutePath().toString());
        region = new SharedMemRegion("test_region", 64 * 1024, true);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (region != null) region.close();
    }

    @Test
    void testWriteAndRead() throws IOException {
        byte[] data = "Hello, SharedMem!".getBytes();
        int written = region.write(data, 0, data.length);
        assertEquals(data.length, written);

        byte[] dst = new byte[data.length];
        int read = region.read(dst, 0, dst.length);
        assertEquals(data.length, read);
        assertArrayEquals(data, dst);
    }

    @Test
    void testMultipleMessages() throws IOException {
        String[] messages = {"alpha", "beta", "gamma", "delta"};
        for (String m : messages) {
            byte[] bytes = m.getBytes();
            assertEquals(bytes.length, region.write(bytes, 0, bytes.length));
        }

        // All writes are raw bytes — concatenated in the ring; read them back sequentially
        for (String expected : messages) {
            byte[] dst = new byte[expected.length()];
            int n = region.read(dst, 0, dst.length);
            assertEquals(expected.length(), n);
            assertEquals(expected, new String(dst));
        }
    }

    @Test
    void testEmptyReadReturnsZero() throws IOException {
        byte[] dst = new byte[256];
        int n = region.read(dst, 0, dst.length);
        assertEquals(0, n, "empty ring should return 0");
    }

    @Test
    void testReadableBytes() throws IOException {
        assertEquals(0, region.readableBytes(), "should be 0 when empty");

        byte[] data = "ping".getBytes();
        region.write(data, 0, data.length);
        assertEquals(data.length, region.readableBytes());

        byte[] dst = new byte[data.length];
        region.read(dst, 0, dst.length);
        assertEquals(0, region.readableBytes(), "should be 0 after full drain");
    }

    @Test
    void testWrapAround() throws IOException {
        int capacity = region.getCapacity();
        int msgLen = capacity / 4;
        byte[] payload = new byte[msgLen];
        Arrays.fill(payload, (byte) 0x42);

        // Write 3 messages, drain them, then write 3 more to exercise wrap-around
        for (int i = 0; i < 3; i++) {
            assertEquals(msgLen, region.write(payload, 0, msgLen));
        }
        for (int i = 0; i < 3; i++) {
            byte[] dst = new byte[msgLen];
            region.read(dst, 0, msgLen);
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(msgLen, region.write(payload, 0, msgLen), "wrap-around write " + i);
        }
        for (int i = 0; i < 3; i++) {
            byte[] dst = new byte[msgLen];
            int n = region.read(dst, 0, msgLen);
            assertEquals(msgLen, n, "wrap-around read " + i);
        }
    }

    @Test
    void testRingFullReturnsPartial() throws IOException {
        int capacity = region.getCapacity();
        byte[] bigPayload = new byte[capacity];
        Arrays.fill(bigPayload, (byte) 0x55);
        // Ring is full after this — write returns less than requested on overflow
        int written = region.write(bigPayload, 0, bigPayload.length);
        assertTrue(written > 0 && written <= capacity);
        assertEquals(0, region.writableBytes());
    }

    @Test
    void testClosedRegionThrows() throws IOException {
        region.close();
        assertThrows(IllegalStateException.class, () ->
                region.write("x".getBytes(), 0, 1));
    }

    @Test
    void testOpenExistingRegion() throws IOException {
        byte[] data = "persist".getBytes();
        region.write(data, 0, data.length);
        region.close();

        try (SharedMemRegion reopened = new SharedMemRegion("test_region", 1, false)) {
            assertEquals(data.length, reopened.readableBytes());
            byte[] dst = new byte[data.length];
            reopened.read(dst, 0, dst.length);
            assertArrayEquals(data, dst);
        }
    }
}
