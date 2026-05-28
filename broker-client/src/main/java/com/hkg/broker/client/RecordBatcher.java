package com.hkg.broker.client;

import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Producer-side accumulator that groups records by destination partition.
 *
 * <p>Tunable via two thresholds:
 * <ul>
 *   <li>{@code batchBytes}: flush a partition's pending batch when its
 *       accumulated payload size reaches this bound. Bigger batches
 *       amortise framing + replication overhead.</li>
 *   <li>{@code lingerMs}: maximum time a record will wait in the
 *       accumulator before being flushed, even if the size threshold has
 *       not been reached. Trades a small latency cost for better
 *       batch utilisation.</li>
 * </ul>
 *
 * <p>The implementation exposes a step function ({@link #drainReady}) so
 * tests can advance time deterministically. The producer client wraps this
 * with a real timer + flush thread in production.
 */
public final class RecordBatcher {

    private final int batchBytes;
    private final long lingerMs;
    private final Map<PartitionId, PendingBatch> pending = new HashMap<>();

    public RecordBatcher(int batchBytes, long lingerMs) {
        if (batchBytes <= 0) throw new IllegalArgumentException("batchBytes must be positive");
        if (lingerMs < 0) throw new IllegalArgumentException("lingerMs must be non-negative");
        this.batchBytes = batchBytes;
        this.lingerMs = lingerMs;
    }

    /** Append a single record destined for {@code partition}. */
    public synchronized void append(PartitionId partition, Record record, long nowMs) {
        PendingBatch batch = pending.computeIfAbsent(partition, p -> new PendingBatch(nowMs));
        batch.add(record);
    }

    /**
     * Return one ready batch per partition: any partition whose pending
     * batch has reached {@code batchBytes} OR has waited at least
     * {@code lingerMs}.
     */
    public synchronized List<Ready> drainReady(long nowMs) {
        List<Ready> ready = new ArrayList<>();
        var it = pending.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            PendingBatch batch = entry.getValue();
            boolean readyOnSize = batch.totalBytes >= batchBytes;
            boolean readyOnLinger = (nowMs - batch.createdAtMs) >= lingerMs;
            if (!batch.records.isEmpty() && (readyOnSize || readyOnLinger)) {
                ready.add(new Ready(entry.getKey(), List.copyOf(batch.records)));
                it.remove();
            }
        }
        return ready;
    }

    /** Force-flush every pending partition regardless of thresholds. */
    public synchronized List<Ready> drainAll() {
        List<Ready> out = new ArrayList<>();
        for (var entry : pending.entrySet()) {
            if (!entry.getValue().records.isEmpty()) {
                out.add(new Ready(entry.getKey(), List.copyOf(entry.getValue().records)));
            }
        }
        pending.clear();
        return out;
    }

    public synchronized int pendingPartitions() {
        return pending.size();
    }

    public record Ready(PartitionId partition, List<Record> records) {}

    private static final class PendingBatch {
        private final List<Record> records = new ArrayList<>();
        private final long createdAtMs;
        private int totalBytes = 0;

        PendingBatch(long createdAtMs) {
            this.createdAtMs = createdAtMs;
        }

        void add(Record r) {
            records.add(r);
            totalBytes += r.sizeBytes();
        }
    }
}
