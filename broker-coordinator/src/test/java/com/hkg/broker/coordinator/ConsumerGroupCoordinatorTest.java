package com.hkg.broker.coordinator;

import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerGroupCoordinatorTest {

    private final Topic topic = new Topic("orders");
    private final List<PartitionId> allPartitions = List.of(
        new PartitionId(topic, 0),
        new PartitionId(topic, 1),
        new PartitionId(topic, 2),
        new PartitionId(topic, 3)
    );

    @Test
    void first_joiner_is_leader_and_triggers_rebalance() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        ConsumerGroupCoordinator.JoinResult join = c.joinGroup("m1", null, allPartitions, 0);
        assertThat(join.leaderId()).isEqualTo("m1");
        assertThat(c.generation()).isEqualTo(1);
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.PREPARING_REBALANCE);
    }

    @Test
    void leader_assigns_partitions_via_sync_group() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.joinGroup("m2", null, allPartitions, 0);
        Map<String, List<PartitionId>> assign = c.defaultAssign(allPartitions);
        // Range-style — 4 partitions across 2 members = 2 each.
        assertThat(assign.get("m1")).hasSize(2);
        assertThat(assign.get("m2")).hasSize(2);
        c.syncGroup("m1", assign);
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.STABLE);
        assertThat(c.assignmentFor("m1")).hasSize(2);
        assertThat(c.assignmentFor("m2")).hasSize(2);
    }

    @Test
    void only_leader_may_submit_assignment() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.joinGroup("m2", null, allPartitions, 0);
        assertThatThrownBy(() ->
            c.syncGroup("m2", c.defaultAssign(allPartitions)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("group leader");
    }

    @Test
    void leave_group_triggers_rebalance() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.joinGroup("m2", null, allPartitions, 0);
        c.syncGroup("m1", c.defaultAssign(allPartitions));
        int gen = c.generation();
        c.leaveGroup("m2");
        assertThat(c.generation()).isEqualTo(gen + 1);
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.PREPARING_REBALANCE);
    }

    @Test
    void expired_heartbeats_evict_member() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 1_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.joinGroup("m2", null, allPartitions, 0);
        c.syncGroup("m1", c.defaultAssign(allPartitions));
        // m1 keeps heartbeating; m2 falls silent past the 1s session timeout.
        c.heartbeat("m1", 4_500);
        Set<String> evicted = c.evictExpired(5_000);
        assertThat(evicted).containsExactly("m2");
        assertThat(c.memberIds()).containsExactly("m1");
    }

    @Test
    void static_member_silent_reconnect_does_not_rebalance() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", "static-pod-0", allPartitions, 0);
        c.joinGroup("m2", "static-pod-1", allPartitions, 0);
        c.syncGroup("m1", c.defaultAssign(allPartitions));
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.STABLE);
        int gen = c.generation();
        // Same static instance reconnects with a new memberId
        ConsumerGroupCoordinator.JoinResult join = c.joinGroup("m1-restart", "static-pod-0", allPartitions, 1_000);
        assertThat(c.generation()).isEqualTo(gen); // no bump
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.STABLE);
        // Existing assignment is returned.
        assertThat(join.assignment()).isNotEmpty();
    }

    @Test
    void cooperative_assignment_preserves_existing_ownership() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.joinGroup("m2", null, allPartitions, 0);
        c.syncGroup("m1", c.defaultAssign(allPartitions));

        PartitionId newPartition = new PartitionId(topic, 4);
        ConsumerGroupCoordinator.CooperativePlan plan =
            c.cooperativeAssign(List.of(
                allPartitions.get(0),
                allPartitions.get(1),
                allPartitions.get(2),
                allPartitions.get(3),
                newPartition
            ));

        assertThat(plan.assignment().get("m1")).contains(allPartitions.get(0), allPartitions.get(1));
        assertThat(plan.assignment().get("m2")).contains(allPartitions.get(2), allPartitions.get(3));
        assertThat(plan.added().values().stream().flatMap(List::stream).toList()).containsExactly(newPartition);
        assertThat(plan.revoked().values().stream().flatMap(List::stream).toList()).isEmpty();
    }

    @Test
    void empty_group_after_all_leave() {
        ConsumerGroupCoordinator c = new ConsumerGroupCoordinator("g", 30_000);
        c.joinGroup("m1", null, allPartitions, 0);
        c.leaveGroup("m1");
        assertThat(c.state()).isEqualTo(ConsumerGroupCoordinator.State.EMPTY);
        assertThat(c.memberIds()).isEmpty();
    }
}
