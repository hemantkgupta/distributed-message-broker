package com.hkg.broker.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordBatchTest {

    @Test
    void encode_then_decode_round_trips() {
        RecordBatch batch = new RecordBatch(
            new Offset(42),
            new LeaderEpoch(3),
            1_000L,
            List.of(
                new Record("k1".getBytes(), "v1".getBytes(), 1_000L),
                new Record(null, "v2".getBytes(), 1_001L),
                new Record("k3".getBytes(), "v3-larger".getBytes(), 1_002L)
            )
        );
        byte[] bytes = batch.toBytes();
        RecordBatch decoded = RecordBatch.fromBytes(bytes);
        assertThat(decoded.baseOffset()).isEqualTo(batch.baseOffset());
        assertThat(decoded.leaderEpoch()).isEqualTo(batch.leaderEpoch());
        assertThat(decoded.firstTimestampMs()).isEqualTo(batch.firstTimestampMs());
        assertThat(decoded.recordCount()).isEqualTo(3);
        assertThat(decoded.records()).containsExactlyElementsOf(batch.records());
    }

    @Test
    void last_offset_is_base_plus_count_minus_one() {
        RecordBatch batch = new RecordBatch(
            new Offset(100),
            LeaderEpoch.INITIAL,
            0L,
            List.of(
                new Record(null, new byte[]{1}, 0L),
                new Record(null, new byte[]{2}, 0L),
                new Record(null, new byte[]{3}, 0L)
            )
        );
        assertThat(batch.lastOffset()).isEqualTo(new Offset(102));
    }

    @Test
    void corrupt_bytes_fail_crc_check() {
        RecordBatch batch = new RecordBatch(
            new Offset(0),
            LeaderEpoch.INITIAL,
            0L,
            List.of(new Record(null, "payload".getBytes(), 0L))
        );
        byte[] bytes = batch.toBytes();
        // Corrupt a payload byte (after the CRC slot)
        bytes[bytes.length - 1] ^= 0x42;
        assertThatThrownBy(() -> RecordBatch.fromBytes(bytes))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("CRC mismatch");
    }

    @Test
    void leader_epoch_change_does_not_invalidate_crc() {
        // The CRC excludes the leader epoch field so it can be stamped on receipt.
        RecordBatch original = new RecordBatch(
            new Offset(0),
            new LeaderEpoch(5),
            0L,
            List.of(new Record(null, "payload".getBytes(), 0L))
        );
        byte[] bytes = original.toBytes();
        // Overwrite the leader epoch in place (offset: 4 length-prefix + 8 baseOffset = bytes 12..15)
        java.nio.ByteBuffer.wrap(bytes).putInt(12, 99);
        RecordBatch decoded = RecordBatch.fromBytes(bytes);
        assertThat(decoded.leaderEpoch()).isEqualTo(new LeaderEpoch(99));
    }

    @Test
    void empty_record_list_rejected() {
        assertThatThrownBy(() -> new RecordBatch(
            new Offset(0), LeaderEpoch.INITIAL, 0L, List.of()
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void total_size_matches_serialized_length() {
        RecordBatch batch = new RecordBatch(
            new Offset(0), LeaderEpoch.INITIAL, 0L,
            List.of(new Record("k".getBytes(), "v".getBytes(), 0L))
        );
        assertThat(batch.toBytes().length).isEqualTo(batch.totalSizeBytes());
    }
}
