package com.hkg.broker.metadata;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataControllerTest {

    @Test
    void register_broker_emits_metadata_record() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "us-east-1a");
        assertThat(ctl.brokers()).containsKey(1);
        assertThat(ctl.metadataLog().size()).isEqualTo(1);
    }

    @Test
    void create_topic_places_replicas_round_robin_and_picks_leader() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "us-east-1a");
        ctl.registerBroker(2, "h2", 9092, "us-east-1b");
        ctl.registerBroker(3, "h3", 9092, "us-east-1c");
        ctl.createTopic("orders", 3, 3);
        var partitions = ctl.partitions();
        assertThat(partitions).hasSize(3);
        for (var entry : partitions.entrySet()) {
            assertThat(entry.getValue().replicas()).hasSize(3);
            assertThat(entry.getValue().isr()).hasSize(3);
            // Leader is the first replica.
            assertThat(entry.getValue().leader()).isEqualTo(entry.getValue().replicas().get(0));
            assertThat(entry.getValue().leaderEpoch()).isEqualTo(LeaderEpoch.INITIAL);
        }
    }

    @Test
    void create_topic_requires_enough_brokers() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "r1");
        ctl.registerBroker(2, "h2", 9092, "r2");
        assertThatThrownBy(() -> ctl.createTopic("t", 1, 3))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("not enough brokers");
    }

    @Test
    void change_leader_bumps_epoch() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "r1");
        ctl.registerBroker(2, "h2", 9092, "r2");
        ctl.registerBroker(3, "h3", 9092, "r3");
        ctl.createTopic("t", 1, 3);
        PartitionId p0 = new PartitionId(new Topic("t"), 0);
        var before = ctl.partitions().get(p0);
        ctl.changeLeader(p0, before.replicas().get(1));
        var after = ctl.partitions().get(p0);
        assertThat(after.leader()).isEqualTo(before.replicas().get(1));
        assertThat(after.leaderEpoch().value()).isEqualTo(before.leaderEpoch().value() + 1);
    }

    @Test
    void change_leader_rejects_broker_not_in_replica_set() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "r1");
        ctl.registerBroker(2, "h2", 9092, "r2");
        ctl.registerBroker(3, "h3", 9092, "r3");
        ctl.registerBroker(4, "h4", 9092, "r4");
        ctl.createTopic("t", 1, 3);
        PartitionId p0 = new PartitionId(new Topic("t"), 0);
        // Broker 4 is registered but, with round-robin RF=3 across {1,2,3,4},
        // partition 0's replica set is exactly {1,2,3}. Broker 4 should
        // therefore be rejected as a leader candidate.
        int outsider = 4;
        assertThat(ctl.partitions().get(p0).replicas()).doesNotContain(outsider);
        assertThatThrownBy(() -> ctl.changeLeader(p0, outsider))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not in replica set");
    }

    @Test
    void brokers_can_tail_from_log_to_learn_state() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "r1");
        ctl.registerBroker(2, "h2", 9092, "r2");
        ctl.registerBroker(3, "h3", 9092, "r3");
        ctl.createTopic("t", 2, 3);

        // Some observing broker starts cold and tails from offset 0.
        List<MetadataLog.Entry> tail = ctl.metadataLog().tailFrom(0);
        long brokerRegs = tail.stream().filter(e -> e.record() instanceof MetadataRecord.RegisterBroker).count();
        long topicCreates = tail.stream().filter(e -> e.record() instanceof MetadataRecord.CreateTopic).count();
        long partitionUpdates = tail.stream().filter(e -> e.record() instanceof MetadataRecord.PartitionLeadership).count();
        assertThat(brokerRegs).isEqualTo(3);
        assertThat(topicCreates).isEqualTo(1);
        assertThat(partitionUpdates).isEqualTo(2);
    }

    @Test
    void update_isr_emits_partition_record() {
        MetadataController ctl = new MetadataController();
        ctl.registerBroker(1, "h1", 9092, "r1");
        ctl.registerBroker(2, "h2", 9092, "r2");
        ctl.registerBroker(3, "h3", 9092, "r3");
        ctl.createTopic("t", 1, 3);
        PartitionId p0 = new PartitionId(new Topic("t"), 0);
        long before = ctl.metadataLog().highWatermark();
        ctl.updateIsr(p0, List.of(ctl.partitions().get(p0).leader()));
        long after = ctl.metadataLog().highWatermark();
        assertThat(after).isEqualTo(before + 1);
        assertThat(ctl.partitions().get(p0).isr()).hasSize(1);
    }
}
