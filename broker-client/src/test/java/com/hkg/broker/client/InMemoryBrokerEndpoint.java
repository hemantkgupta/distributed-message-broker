package com.hkg.broker.client;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal {@link BrokerEndpoint} backed by an in-memory list of
 * RecordBatch per partition. Used as a stub for client tests so this
 * module doesn't need to depend on broker-storage / broker-replica.
 *
 * <p>This does NOT enforce HWM (everything written is immediately
 * visible). The simulator-level tests in Phase 5 exercise the full
 * replica + HWM semantics.
 */
final class InMemoryBrokerEndpoint implements BrokerEndpoint {

    private final Map<PartitionId, List<RecordBatch>> store = new HashMap<>();
    private final Map<PartitionId, Long> nextOffsets = new HashMap<>();

    @Override
    public synchronized ProduceResult produce(PartitionId partition, List<Record> records) throws IOException {
        long base = nextOffsets.getOrDefault(partition, 0L);
        RecordBatch batch = new RecordBatch(
            new Offset(base), LeaderEpoch.INITIAL, records.get(0).timestampMs(), records);
        store.computeIfAbsent(partition, k -> new ArrayList<>()).add(batch);
        nextOffsets.put(partition, base + records.size());
        return new ProduceResult(new Offset(base), new Offset(base + records.size() - 1));
    }

    @Override
    public synchronized List<RecordBatch> fetch(PartitionId partition, Offset from, int maxBytes) throws IOException {
        List<RecordBatch> all = store.getOrDefault(partition, List.of());
        List<RecordBatch> out = new ArrayList<>();
        int accumulated = 0;
        for (RecordBatch b : all) {
            if (b.lastOffset().compareTo(from) < 0) continue;
            if (accumulated >= maxBytes) break;
            out.add(b);
            accumulated += b.toBytes().length;
        }
        return out;
    }
}
