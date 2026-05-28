package com.hkg.broker.client;

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
 * Pull-based consumer client.
 *
 * <p>Maintains an offset position per assigned partition; each {@link #poll}
 * call issues fetch requests against the broker endpoint and advances the
 * per-partition position. Records past the broker's high watermark are
 * invisible (per {@code PartitionReplica.fetch}'s HWM guard), so the
 * consumer naturally sees committed-only data.
 *
 * <p>Offset commit is exposed as {@link #position} + an external commit
 * call (typically through the group coordinator's {@code OffsetManager}).
 * This class intentionally does not own commit policy — the caller decides
 * whether to commit on every poll, on a timer, or at-least-once.
 */
public final class ConsumerClient {

    private final BrokerEndpoint endpoint;
    private final int maxFetchBytes;
    private final Map<PartitionId, Offset> positions = new HashMap<>();

    public ConsumerClient(BrokerEndpoint endpoint, int maxFetchBytes) {
        this.endpoint = endpoint;
        this.maxFetchBytes = maxFetchBytes;
    }

    /**
     * Assign a partition starting at {@code initialOffset}. Replaces any
     * previous assignment for the partition.
     */
    public void assign(PartitionId partition, Offset initialOffset) {
        positions.put(partition, initialOffset);
    }

    public void unassign(PartitionId partition) {
        positions.remove(partition);
    }

    public Offset position(PartitionId partition) {
        return positions.getOrDefault(partition, Offset.ZERO);
    }

    /**
     * Issue fetch requests for every assigned partition and return any
     * records received. Advances the per-partition position past the
     * last received record.
     */
    public List<Polled> poll() throws IOException {
        List<Polled> result = new ArrayList<>();
        for (Map.Entry<PartitionId, Offset> entry : positions.entrySet()) {
            PartitionId p = entry.getKey();
            Offset from = entry.getValue();
            List<RecordBatch> batches = endpoint.fetch(p, from, maxFetchBytes);
            for (RecordBatch b : batches) {
                for (int i = 0; i < b.records().size(); i++) {
                    Record r = b.records().get(i);
                    Offset off = b.baseOffset().plus(i);
                    if (off.compareTo(from) >= 0) {
                        result.add(new Polled(p, off, r));
                    }
                }
                if (b.lastOffset().compareTo(positions.get(p)) >= 0) {
                    positions.put(p, b.lastOffset().plus(1));
                }
            }
        }
        return result;
    }

    public record Polled(PartitionId partition, Offset offset, Record record) {}
}
