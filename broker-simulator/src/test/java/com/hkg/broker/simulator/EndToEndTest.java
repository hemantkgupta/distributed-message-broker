package com.hkg.broker.simulator;

import com.hkg.broker.client.BrokerEndpoint;
import com.hkg.broker.client.ConsumerClient;
import com.hkg.broker.client.ProducerClient;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.Topic;
import com.hkg.broker.replica.PartitionReplica;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndToEndTest {

    private Record rec(String value) {
        return new Record(null, value.getBytes(), 0L);
    }

    @Test
    void produce_then_consume_with_replication(@TempDir Path tmp) throws IOException {
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("orders", /* partitions */ 1, /* RF */ 3, /* lagMs */ 30_000)) {

            PartitionId p0 = new PartitionId(new Topic("orders"), 0);
            ReplicaEndpoint endpoint = new ReplicaEndpoint(h.leaders());
            ProducerClient producer = new ProducerClient(endpoint, /* batchBytes */ 4096, /* linger */ 0);
            ConsumerClient consumer = new ConsumerClient(endpoint, 4096);
            consumer.assign(p0, Offset.ZERO);

            for (int i = 0; i < 5; i++) producer.send(p0, rec("r" + i), 0);
            producer.flushAll();

            // Records are NOT visible until followers replicate (HWM bound).
            assertThat(consumer.poll()).isEmpty();

            // Drive replication to advance the HWM.
            int replicated = h.catchUp();
            assertThat(replicated).isGreaterThan(0);
            assertThat(h.leaderOf(p0).highWatermark()).isEqualTo(new Offset(5));

            List<ConsumerClient.Polled> polled = consumer.poll();
            assertThat(polled).hasSize(5);
            assertThat(polled.stream().map(p -> new String(p.record().value())).toList())
                .containsExactly("r0", "r1", "r2", "r3", "r4");
        }
    }

    @Test
    void leader_change_bumps_epoch_in_metadata_log(@TempDir Path tmp) throws IOException {
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("t", 1, 3, 30_000)) {

            PartitionId p0 = new PartitionId(new Topic("t"), 0);
            var state = h.metadataController().partitions().get(p0);
            int originalLeader = state.leader();
            int newLeader = state.replicas().stream()
                .filter(id -> id != originalLeader).findFirst().orElseThrow();

            h.metadataController().changeLeader(p0, newLeader);
            var after = h.metadataController().partitions().get(p0);
            assertThat(after.leader()).isEqualTo(newLeader);
            assertThat(after.leaderEpoch().value()).isEqualTo(state.leaderEpoch().value() + 1);
        }
    }

    @Test
    void network_partition_isolates_produce_path(@TempDir Path tmp) throws IOException {
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("t", 1, 3, 30_000)) {

            PartitionId p0 = new PartitionId(new Topic("t"), 0);
            ReplicaEndpoint endpoint = new ReplicaEndpoint(h.leaders());
            endpoint.filter().cut(p0);

            ProducerClient producer = new ProducerClient(endpoint, 4096, 0);
            producer.send(p0, rec("blocked"), 0);
            assertThatThrownBy(producer::flushAll)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("network partition");

            endpoint.filter().heal(p0);
            // Buffer was drained on the failed flush, so re-send before retrying.
            producer.send(p0, rec("recovered"), 0);
            producer.flushAll();
            h.catchUp();
            assertThat(h.leaderOf(p0).highWatermark().value()).isEqualTo(1L);
        }
    }

    @Test
    void shrunken_isr_advances_hwm_without_lagging_follower(@TempDir Path tmp) throws IOException {
        // Use a short lag threshold so we can deliberately let one follower fall behind.
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("t", 1, 3, /* replicaLagMs */ 1)) {

            PartitionId p0 = new PartitionId(new Topic("t"), 0);
            PartitionReplica leader = h.leaderOf(p0);

            ReplicaEndpoint endpoint = new ReplicaEndpoint(h.leaders());
            ProducerClient producer = new ProducerClient(endpoint, 4096, 0);
            producer.send(p0, rec("x"), 0);
            producer.send(p0, rec("y"), 0);
            producer.flushAll();
            // Followers haven't fetched yet; HWM is at 0.
            assertThat(leader.highWatermark()).isEqualTo(Offset.ZERO);

            // Force the ISR to shrink to just the leader.
            leader.shrinkIsrForLaggingFollowers(System.currentTimeMillis() + 10_000);
            assertThat(leader.isr()).containsExactly(leader.brokerId());
            assertThat(leader.highWatermark()).isEqualTo(new Offset(2));
        }
    }

    @Test
    void cluster_state_visible_to_observers_via_metadata_log(@TempDir Path tmp) throws IOException {
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("t", 2, 3, 30_000)) {

            // An observing broker can reconstruct cluster state from the
            // metadata log alone — no direct RPC to the controller.
            var tail = h.metadataController().metadataLog().tailFrom(0);
            long brokerRegs = tail.stream()
                .filter(e -> e.record() instanceof com.hkg.broker.metadata.MetadataRecord.RegisterBroker).count();
            long createTopics = tail.stream()
                .filter(e -> e.record() instanceof com.hkg.broker.metadata.MetadataRecord.CreateTopic).count();
            long partitionRecords = tail.stream()
                .filter(e -> e.record() instanceof com.hkg.broker.metadata.MetadataRecord.PartitionLeadership).count();
            assertThat(brokerRegs).isEqualTo(3);
            assertThat(createTopics).isEqualTo(1);
            assertThat(partitionRecords).isEqualTo(2);
        }
    }

    @Test
    void follower_fetching_serves_from_nearest_in_sync_replica(@TempDir Path tmp) throws IOException {
        try (ClusterHarness h = new ClusterHarness(tmp)
            .withBrokers(3)
            .createTopic("orders", 1, 3, 30_000)) {

            PartitionId p0 = new PartitionId(new Topic("orders"), 0);
            ReplicaEndpoint leaderEndpoint = new ReplicaEndpoint(h.leaders());
            ProducerClient producer = new ProducerClient(leaderEndpoint, 4096, 0);
            producer.send(p0, rec("local-rack-read"), 0);
            producer.flushAll();
            h.catchUp();

            PartitionReplica nearest = h.preferredFetchReplica(p0, "rack2");
            assertThat(nearest.brokerId()).isEqualTo(2);
            assertThat(nearest.role()).isEqualTo(PartitionReplica.Role.FOLLOWER);
            assertThat(nearest.highWatermark()).isEqualTo(h.leaderOf(p0).highWatermark());

            ReplicaEndpoint rackAwareEndpoint = new ReplicaEndpoint(
                h.leaders(),
                h.replicasByPartition(),
                h.metadataController(),
                "rack2"
            );
            ConsumerClient consumer = new ConsumerClient(rackAwareEndpoint, 4096);
            consumer.assign(p0, Offset.ZERO);
            List<ConsumerClient.Polled> polled = consumer.poll();
            assertThat(polled).hasSize(1);
            assertThat(new String(polled.get(0).record().value())).isEqualTo("local-rack-read");
        }
    }
}
