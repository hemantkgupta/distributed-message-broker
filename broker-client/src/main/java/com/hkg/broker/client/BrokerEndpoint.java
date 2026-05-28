package com.hkg.broker.client;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.util.List;

/**
 * Abstract broker endpoint the client talks to. In the simulator (Phase 5)
 * this is wired directly to {@code PartitionReplica}; in a real deployment
 * the implementation would marshal RPCs over a socket using the framing in
 * {@code broker-network}.
 */
public interface BrokerEndpoint {

    /**
     * Send a batch of records to the leader of the given partition.
     * Returns the assigned base + last offset on success.
     */
    ProduceResult produce(PartitionId partition, List<Record> records) throws IOException;

    /**
     * Fetch up to {@code maxBytes} of batches from the given partition,
     * starting at the requested offset. Returns an empty list when there
     * is no new data visible (i.e., the requested offset is at or above
     * the high watermark).
     */
    List<RecordBatch> fetch(PartitionId partition, Offset from, int maxBytes) throws IOException;

    record ProduceResult(Offset baseOffset, Offset lastOffset) {}
}
