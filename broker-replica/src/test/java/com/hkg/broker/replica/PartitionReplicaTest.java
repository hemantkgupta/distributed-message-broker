package com.hkg.broker.replica;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;
import com.hkg.broker.common.Topic;
import com.hkg.broker.storage.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionReplicaTest {

    private PartitionId pid() {
        return new PartitionId(new Topic("t"), 0);
    }

    private Log openLog(Path dir) throws IOException {
        return new Log(pid(), dir, 64 * 1024, Long.MAX_VALUE, 128);
    }

    private List<Record> oneRecord() {
        return List.of(new Record(null, "payload".getBytes(), 0L));
    }

    @Test
    void produce_advances_leader_LEO_and_HWM_when_single_isr(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica leader = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1), /* lagMs */ 30_000
            );
            leader.produce(oneRecord());
            assertThat(leader.logEndOffset()).isEqualTo(new Offset(1));
            assertThat(leader.highWatermark()).isEqualTo(new Offset(1));
        }
    }

    @Test
    void hwm_lags_behind_LEO_when_follower_not_caught_up(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica leader = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), /* lagMs */ 30_000
            );
            leader.produce(oneRecord());
            leader.produce(oneRecord());
            // Follower hasn't fetched yet — its LEO is 0
            assertThat(leader.logEndOffset()).isEqualTo(new Offset(2));
            assertThat(leader.highWatermark()).isEqualTo(Offset.ZERO);
            // Follower catches up to offset 1
            leader.onFollowerFetch(2, new Offset(1));
            assertThat(leader.highWatermark()).isEqualTo(new Offset(1));
            // And to offset 2
            leader.onFollowerFetch(2, new Offset(2));
            assertThat(leader.highWatermark()).isEqualTo(new Offset(2));
        }
    }

    @Test
    void consumer_fetch_returns_nothing_above_hwm(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica leader = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), /* lagMs */ 30_000
            );
            leader.produce(oneRecord());
            leader.produce(oneRecord());
            // HWM is still 0; consumer fetch from 0 sees nothing.
            assertThat(leader.fetch(Offset.ZERO, 1024)).isEmpty();
            // Once follower catches up, the records become visible.
            leader.onFollowerFetch(2, new Offset(2));
            List<RecordBatch> visible = leader.fetch(Offset.ZERO, 1024);
            assertThat(visible).hasSize(2);
        }
    }

    @Test
    void lagging_follower_shrinks_isr_and_advances_hwm(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica leader = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), /* lagMs */ 1_000
            );
            leader.produce(oneRecord());
            // Without a follower fetch, HWM stays at 0.
            assertThat(leader.highWatermark()).isEqualTo(Offset.ZERO);
            // Simulate time passing past the lag threshold.
            leader.shrinkIsrForLaggingFollowers(System.currentTimeMillis() + 5_000L);
            assertThat(leader.isr()).containsOnly(1);
            assertThat(leader.highWatermark()).isEqualTo(new Offset(1));
        }
    }

    @Test
    void min_insync_replicas_floor_rejects_produce(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica leader = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1), /* lagMs */ 30_000
            );
            leader.setMinInsyncReplicas(2);
            assertThatThrownBy(() -> leader.produce(oneRecord()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("insufficient in-sync replicas");
        }
    }

    @Test
    void become_leader_records_new_epoch_in_tracker(@TempDir Path tmp) throws IOException {
        try (Log log = openLog(tmp)) {
            PartitionReplica replica = new PartitionReplica(
                pid(), 1, log,
                PartitionReplica.Role.FOLLOWER, new LeaderEpoch(1),
                Set.of(1, 2), /* lagMs */ 30_000
            );
            replica.becomeLeader(new LeaderEpoch(2), Set.of(1, 2));
            assertThat(replica.role()).isEqualTo(PartitionReplica.Role.LEADER);
            assertThat(replica.epochTracker().latestEpoch()).contains(new LeaderEpoch(2));
        }
    }
}
