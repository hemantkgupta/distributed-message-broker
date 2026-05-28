package com.hkg.broker.client;

import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordBatcherTest {

    private final PartitionId p0 = new PartitionId(new Topic("t"), 0);
    private final PartitionId p1 = new PartitionId(new Topic("t"), 1);

    private Record sized(int payloadBytes) {
        return new Record(null, new byte[payloadBytes], 0L);
    }

    @Test
    void linger_keeps_records_pending() {
        RecordBatcher b = new RecordBatcher(/* batchBytes */ 10_000, /* linger */ 1_000);
        b.append(p0, sized(16), 0);
        // Within linger window, nothing is ready.
        assertThat(b.drainReady(500)).isEmpty();
        assertThat(b.pendingPartitions()).isOne();
        // Past linger window — the batch becomes ready.
        assertThat(b.drainReady(1_500)).hasSize(1);
        assertThat(b.pendingPartitions()).isZero();
    }

    @Test
    void size_threshold_fires_before_linger() {
        RecordBatcher b = new RecordBatcher(/* batchBytes */ 60, /* linger */ 1_000);
        // sizeBytes() for a Record(null, 32B value, ts) = 4 + 4 + 8 + 0 + 32 = 48
        b.append(p0, sized(32), 0);
        // Below threshold (48 < 60) — still pending.
        assertThat(b.drainReady(0)).isEmpty();
        b.append(p0, sized(32), 0);
        // Now 96 > 60 — ready immediately.
        List<RecordBatcher.Ready> ready = b.drainReady(0);
        assertThat(ready).hasSize(1);
        assertThat(ready.get(0).records()).hasSize(2);
    }

    @Test
    void multiple_partitions_drain_independently() {
        RecordBatcher b = new RecordBatcher(/* batchBytes */ 60, /* linger */ 1_000);
        b.append(p0, sized(64), 0); // immediately ready
        b.append(p1, sized(8), 0);  // below threshold
        List<RecordBatcher.Ready> ready = b.drainReady(0);
        assertThat(ready).hasSize(1);
        assertThat(ready.get(0).partition()).isEqualTo(p0);
        assertThat(b.pendingPartitions()).isOne();
    }

    @Test
    void drain_all_flushes_regardless_of_thresholds() {
        RecordBatcher b = new RecordBatcher(10_000, 1_000);
        b.append(p0, sized(16), 0);
        b.append(p1, sized(16), 0);
        assertThat(b.drainAll()).hasSize(2);
        assertThat(b.pendingPartitions()).isZero();
    }

    @Test
    void invalid_arguments_rejected() {
        assertThatThrownBy(() -> new RecordBatcher(0, 100)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RecordBatcher(64, -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
