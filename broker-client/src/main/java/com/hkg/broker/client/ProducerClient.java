package com.hkg.broker.client;

import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer client wrapping a {@link RecordBatcher} + a {@link BrokerEndpoint}.
 *
 * <p>Records are sent into the batcher. The flush methods drain ready
 * batches and submit them to the endpoint. The simulator (Phase 5) calls
 * {@link #flushReady} periodically; production code would wire the same
 * call to a background sender thread on a timer.
 *
 * <p>The {@code producerEpoch} is the seed for idempotency: it is bumped
 * once per {@code initProducer()} call. A real Kafka producer would obtain
 * this from the broker via {@code InitProducerId}; here we simulate by
 * incrementing locally.
 */
public final class ProducerClient {

    private final RecordBatcher batcher;
    private final BrokerEndpoint endpoint;
    private final AtomicLong producerEpoch = new AtomicLong(0);

    public ProducerClient(BrokerEndpoint endpoint, int batchBytes, long lingerMs) {
        this.endpoint = endpoint;
        this.batcher = new RecordBatcher(batchBytes, lingerMs);
    }

    public long initProducer() {
        return producerEpoch.incrementAndGet();
    }

    /** Buffer a record. Does not immediately send. */
    public void send(PartitionId partition, Record record, long nowMs) {
        batcher.append(partition, record, nowMs);
    }

    /** Drain ready batches at the given clock and send them to brokers. */
    public List<BrokerEndpoint.ProduceResult> flushReady(long nowMs) throws IOException {
        List<RecordBatcher.Ready> ready = batcher.drainReady(nowMs);
        List<BrokerEndpoint.ProduceResult> results = new ArrayList<>();
        for (RecordBatcher.Ready r : ready) {
            results.add(endpoint.produce(r.partition(), r.records()));
        }
        return results;
    }

    /** Force-flush every pending partition regardless of thresholds. */
    public List<BrokerEndpoint.ProduceResult> flushAll() throws IOException {
        List<RecordBatcher.Ready> ready = batcher.drainAll();
        List<BrokerEndpoint.ProduceResult> results = new ArrayList<>();
        for (RecordBatcher.Ready r : ready) {
            results.add(endpoint.produce(r.partition(), r.records()));
        }
        return results;
    }

    public int pendingPartitions() {
        return batcher.pendingPartitions();
    }

    public long producerEpoch() {
        return producerEpoch.get();
    }
}
