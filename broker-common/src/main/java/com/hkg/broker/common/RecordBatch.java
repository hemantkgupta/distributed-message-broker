package com.hkg.broker.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * A batch of records that is the unit of replication, compression, and
 * zero-copy transfer.
 *
 * <p>The wire format is identical to the on-disk segment format. The broker
 * appends the bytes verbatim and never decodes the payload during the read
 * path. Followers replicate by streaming the same bytes the leader wrote.
 *
 * <p>Frame layout (length-prefixed):
 * <pre>
 *   int32   batchLength             (everything after this field)
 *   int64   baseOffset              (assigned by the leader at append time)
 *   int32   partitionLeaderEpoch    (excluded from CRC)
 *   int32   recordCount
 *   int64   firstTimestampMs
 *   uint32  crc32c                  (computed over everything below this line)
 *   { for each record:
 *       int32 keyLen ( -1 == null )
 *       int32 valLen
 *       int64 timestampMs
 *       bytes key  [keyLen]
 *       bytes value [valLen]
 *   }
 * </pre>
 *
 * <p>The {@code partitionLeaderEpoch} is intentionally excluded from the CRC so
 * the leader can stamp it after the producer has computed the checksum without
 * forcing a re-compute on receipt. This preserves zero-copy on the read path.
 */
public record RecordBatch(
    Offset baseOffset,
    LeaderEpoch leaderEpoch,
    long firstTimestampMs,
    List<Record> records
) {

    public RecordBatch {
        Objects.requireNonNull(baseOffset, "baseOffset");
        Objects.requireNonNull(leaderEpoch, "leaderEpoch");
        Objects.requireNonNull(records, "records");
        if (records.isEmpty()) {
            throw new IllegalArgumentException("batch must contain at least one record");
        }
        records = List.copyOf(records);
    }

    public int recordCount() {
        return records.size();
    }

    public Offset lastOffset() {
        return baseOffset.plus(records.size() - 1L);
    }

    /**
     * Serialise to bytes using the canonical wire format. The output is also
     * the on-disk format.
     */
    public byte[] toBytes() {
        int payloadSize = 0;
        for (Record r : records) {
            int keyLen = r.key().map(k -> k.length).orElse(-1);
            int valLen = r.value().length;
            payloadSize += 4 + 4 + 8;
            if (keyLen >= 0) payloadSize += keyLen;
            payloadSize += valLen;
        }
        // header below batchLength: baseOffset(8) + epoch(4) + count(4) + ts(8) + crc(4) = 28
        int batchLength = 28 + payloadSize;
        ByteBuffer buf = ByteBuffer.allocate(4 + batchLength);
        buf.putInt(batchLength);
        buf.putLong(baseOffset.value());
        buf.putInt(leaderEpoch.value());
        buf.putInt(records.size());
        buf.putLong(firstTimestampMs);
        int crcSlot = buf.position();
        buf.putInt(0); // crc placeholder
        for (Record r : records) {
            byte[] key = r.key().orElse(null);
            byte[] val = r.value();
            buf.putInt(key == null ? -1 : key.length);
            buf.putInt(val.length);
            buf.putLong(r.timestampMs());
            if (key != null) buf.put(key);
            buf.put(val);
        }
        // CRC over bytes after the crc slot
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), crcSlot + 4, buf.array().length - (crcSlot + 4));
        buf.putInt(crcSlot, (int) crc.getValue());
        return buf.array();
    }

    /** Parse a single batch from the start of {@code bytes}. */
    public static RecordBatch fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int batchLength = buf.getInt();
        if (4 + batchLength > bytes.length) {
            throw new IllegalArgumentException("truncated batch: declared length " + batchLength
                + " exceeds buffer " + (bytes.length - 4));
        }
        long base = buf.getLong();
        int epoch = buf.getInt();
        int count = buf.getInt();
        long ts = buf.getLong();
        int crcStored = buf.getInt();
        int recordStart = buf.position();

        CRC32C crc = new CRC32C();
        crc.update(bytes, recordStart, batchLength - 28);
        int crcCalc = (int) crc.getValue();
        if (crcCalc != crcStored) {
            throw new IllegalStateException("CRC mismatch: stored=" + crcStored + " calc=" + crcCalc);
        }
        Record[] records = new Record[count];
        for (int i = 0; i < count; i++) {
            int keyLen = buf.getInt();
            int valLen = buf.getInt();
            long recTs = buf.getLong();
            byte[] key = null;
            if (keyLen >= 0) {
                key = new byte[keyLen];
                buf.get(key);
            }
            byte[] val = new byte[valLen];
            buf.get(val);
            records[i] = new Record(key, val, recTs);
        }
        return new RecordBatch(new Offset(base), new LeaderEpoch(epoch), ts, List.of(records));
    }

    /** Total wire-bytes including the 4-byte length prefix. */
    public int totalSizeBytes() {
        return 4 + 28 + records.stream().mapToInt(r ->
            8 + 4 + 4
                + r.key().map(k -> k.length).orElse(0)
                + r.value().length
        ).sum();
    }
}
