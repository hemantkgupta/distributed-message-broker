package com.hkg.broker.client;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProducerConsumerTest {

    private final PartitionId p0 = new PartitionId(new Topic("t"), 0);

    private Record rec(String value) {
        return new Record(null, value.getBytes(), 0L);
    }

    @Test
    void producer_flush_all_round_trips_to_consumer() throws IOException {
        InMemoryBrokerEndpoint endpoint = new InMemoryBrokerEndpoint();
        ProducerClient producer = new ProducerClient(endpoint, /* batchBytes */ 4096, /* linger */ 0);
        ConsumerClient consumer = new ConsumerClient(endpoint, 4096);
        consumer.assign(p0, Offset.ZERO);

        for (int i = 0; i < 5; i++) producer.send(p0, rec("r" + i), 0);
        producer.flushAll();

        List<ConsumerClient.Polled> polled = consumer.poll();
        assertThat(polled).hasSize(5);
        assertThat(polled.stream().map(p -> new String(p.record().value())).toList())
            .containsExactly("r0", "r1", "r2", "r3", "r4");
        assertThat(consumer.position(p0)).isEqualTo(new Offset(5));
    }

    @Test
    void consumer_resumes_from_position() throws IOException {
        InMemoryBrokerEndpoint endpoint = new InMemoryBrokerEndpoint();
        ProducerClient producer = new ProducerClient(endpoint, 4096, 0);
        producer.send(p0, rec("a"), 0);
        producer.send(p0, rec("b"), 0);
        producer.send(p0, rec("c"), 0);
        producer.flushAll();

        ConsumerClient first = new ConsumerClient(endpoint, 4096);
        first.assign(p0, Offset.ZERO);
        assertThat(first.poll()).hasSize(3);

        ConsumerClient resumer = new ConsumerClient(endpoint, 4096);
        resumer.assign(p0, new Offset(2));
        List<ConsumerClient.Polled> tail = resumer.poll();
        assertThat(tail).hasSize(1);
        assertThat(new String(tail.get(0).record().value())).isEqualTo("c");
    }

    @Test
    void init_producer_bumps_epoch_for_idempotency() {
        InMemoryBrokerEndpoint endpoint = new InMemoryBrokerEndpoint();
        ProducerClient producer = new ProducerClient(endpoint, 4096, 0);
        long e1 = producer.initProducer();
        long e2 = producer.initProducer();
        assertThat(e2).isGreaterThan(e1);
        assertThat(producer.producerEpoch()).isEqualTo(e2);
    }

    @Test
    void flush_ready_respects_linger() throws IOException {
        InMemoryBrokerEndpoint endpoint = new InMemoryBrokerEndpoint();
        ProducerClient producer = new ProducerClient(endpoint, /* batchBytes */ 65_536, /* linger */ 100);
        producer.send(p0, rec("a"), /* now */ 0);
        // Below linger threshold — no flush.
        assertThat(producer.flushReady(50)).isEmpty();
        // Past linger threshold — flushed.
        assertThat(producer.flushReady(150)).hasSize(1);
    }
}
