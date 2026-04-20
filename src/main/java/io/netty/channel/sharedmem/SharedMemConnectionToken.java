package io.netty.channel.sharedmem;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A fixed-size binary token written by a connecting client into the server's
 * CQ region ({@code regionName + ".cq"}) to request a connection.
 *
 * <p>The client writes this token <b>after</b> it has fully created its own
 * dedicated data region.  The server reads the token from the CQ, opens the
 * data region (already initialised by the client), and creates a child channel.
 *
 * <h3>Wire format — exactly {@value #ENCODED_SIZE} bytes, big-endian</h3>
 * <pre>
 * Offset  Size  Type     Field
 * ──────  ────  ───────  ──────────────────────────────────────────────
 *  0       4    int32    Magic      (0x43544F4B = ASCII "CTOK")
 *  4       4    int32    Client port
 *  8       4    int32    Region capacity in bytes  ← NEW: tells the server
 *                                                    how large the data region is
 * 12       2    uint16   Region name length (UTF-8 bytes)
 * 14     110    bytes    Region name (UTF-8, zero-padded to 110 bytes)
 * ──────  ────
 * Total = 128 bytes
 * </pre>
 */
public final class SharedMemConnectionToken {

    /** Total encoded size of one token in bytes — fixed at 128. */
    public static final int ENCODED_SIZE = 128;

    /** Magic number: ASCII "CTOK". */
    public static final int MAGIC = 0x43544F4B;

    /**
     * Maximum UTF-8 byte length of the region name field.
     * 128 total − 14 bytes of fixed header = 114 bytes, but we keep 4 bytes of
     * padding → 110 usable bytes.
     */
    public static final int MAX_NAME_BYTES = 110;

    // Wire offsets
    private static final int OFF_MAGIC    =  0;
    private static final int OFF_PORT     =  4;
    private static final int OFF_CAPACITY =  8;
    private static final int OFF_NAME_LEN = 12;
    private static final int OFF_NAME     = 14;

    // ─────────────────────────────────────────────────────────────────────────
    // Fields
    // ─────────────────────────────────────────────────────────────────────────

    private final String regionName;
    private final int    port;
    private final int    regionCapacity;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a connection token.
     *
     * @param regionName     the client's dedicated data region name; UTF-8 encoding
     *                       must not exceed {@value #MAX_NAME_BYTES} bytes
     * @param port           the client's logical port
     * @param regionCapacity the byte capacity of the data region the client created;
     *                       the server uses this to open the region with the correct size
     * @throws IllegalArgumentException if any argument is invalid
     */
    public SharedMemConnectionToken(String regionName, int port, int regionCapacity) {
        if (regionName == null || regionName.isBlank()) {
            throw new IllegalArgumentException("regionName must not be null or blank");
        }
        byte[] nameBytes = regionName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException(
                    "regionName UTF-8 length " + nameBytes.length +
                            " exceeds maximum " + MAX_NAME_BYTES);
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range [0,65535]: " + port);
        }
        if (regionCapacity <= 0) {
            throw new IllegalArgumentException("regionCapacity must be > 0");
        }
        this.regionName     = regionName;
        this.port           = port;
        this.regionCapacity = regionCapacity;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /** The name of the client's dedicated data region. */
    public String getRegionName()     { return regionName;     }

    /** The client's logical port. */
    public int    getPort()           { return port;           }

    /**
     * The byte capacity of the data region the client created.
     * The server passes this to {@link SharedMemRegion} when opening the region.
     */
    public int    getRegionCapacity() { return regionCapacity; }

    // ─────────────────────────────────────────────────────────────────────────
    // Encode / decode
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Encodes this token to a {@value #ENCODED_SIZE}-byte array.
     */
    public byte[] encode() {
        byte[]     nameBytes = regionName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf       = ByteBuffer.allocate(ENCODED_SIZE); // big-endian default

        buf.putInt(OFF_MAGIC,    MAGIC);
        buf.putInt(OFF_PORT,     port);
        buf.putInt(OFF_CAPACITY, regionCapacity);
        buf.putShort(OFF_NAME_LEN, (short) nameBytes.length);
        buf.position(OFF_NAME);
        buf.put(nameBytes);
        // remaining bytes are zero-filled by ByteBuffer.allocate()
        return buf.array();
    }

    /**
     * Decodes a token from a {@value #ENCODED_SIZE}-byte array.
     *
     * @param data source bytes; must be at least {@value #ENCODED_SIZE} bytes long
     * @return decoded token
     * @throws IllegalArgumentException if the magic is wrong or name length overflows
     */
    public static SharedMemConnectionToken decode(byte[] data) {
        if (data == null || data.length < ENCODED_SIZE) {
            throw new IllegalArgumentException(
                    "Buffer too short: need " + ENCODED_SIZE +
                            " bytes, got " + (data == null ? 0 : data.length));
        }

        ByteBuffer buf   = ByteBuffer.wrap(data); // big-endian
        int        magic = buf.getInt(OFF_MAGIC);
        if (magic != MAGIC) {
            throw new IllegalArgumentException(String.format(
                    "Bad magic: expected 0x%08X got 0x%08X", MAGIC, magic));
        }

        int    port           = buf.getInt(OFF_PORT);
        int    regionCapacity = buf.getInt(OFF_CAPACITY);
        int    nameLen        = buf.getShort(OFF_NAME_LEN) & 0xFFFF;
        if (nameLen > MAX_NAME_BYTES) {
            throw new IllegalArgumentException(
                    "Encoded name length " + nameLen + " exceeds max " + MAX_NAME_BYTES);
        }

        byte[] nameBytes = new byte[nameLen];
        buf.position(OFF_NAME);
        buf.get(nameBytes);

        return new SharedMemConnectionToken(
                new String(nameBytes, StandardCharsets.UTF_8),
                port,
                regionCapacity);
    }

    @Override
    public String toString() {
        return "SharedMemConnectionToken{region='" + regionName +
                "', port=" + port +
                ", capacity=" + regionCapacity + '}';
    }
}