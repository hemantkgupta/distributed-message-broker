package com.hkg.broker.replica;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.Topic;
import com.hkg.broker.storage.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationManagerTest {

    private List<Record> rec() {
        return List.of(new Record(null, "payload".getBytes(), 0L));
    }

    @Test
    void follower_catches_up_to_leader(@TempDir Path tmp) throws IOException {
        PartitionId pid = new PartitionId(new Topic("t"), 0);
        try (Log leaderLog = new Log(pid, tmp.resolve("leader"), 64 * 1024, Long.MAX_VALUE, 128);
             Log followerLog = new Log(pid, tmp.resolve("follower"), 64 * 1024, Long.MAX_VALUE, 128)) {

            PartitionReplica leader = new PartitionReplica(
                pid, 1, leaderLog,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            PartitionReplica follower = new PartitionReplica(
                pid, 2, followerLog,
                PartitionReplica.Role.FOLLOWER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            ReplicationManager mgr = new ReplicationManager(follower, leader, 4096);

            for (int i = 0; i < 5; i++) {
                leader.produce(rec());
            }
            int replicated = mgr.catchUp();
            assertThat(replicated).isEqualTo(5);
            assertThat(follower.logEndOffset()).isEqualTo(new Offset(5));
            // HWM on leader now reflects follower having caught up.
            assertThat(leader.highWatermark()).isEqualTo(new Offset(5));
        }
    }

    @Test
    void replica_fetch_advances_hwm(@TempDir Path tmp) throws IOException {
        PartitionId pid = new PartitionId(new Topic("t"), 0);
        try (Log leaderLog = new Log(pid, tmp.resolve("leader"), 64 * 1024, Long.MAX_VALUE, 128);
             Log followerLog = new Log(pid, tmp.resolve("follower"), 64 * 1024, Long.MAX_VALUE, 128)) {

            PartitionReplica leader = new PartitionReplica(
                pid, 1, leaderLog,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            PartitionReplica follower = new PartitionReplica(
                pid, 2, followerLog,
                PartitionReplica.Role.FOLLOWER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            ReplicationManager mgr = new ReplicationManager(follower, leader, 4096);

            leader.produce(rec());
            leader.produce(rec());
            assertThat(leader.highWatermark()).isEqualTo(Offset.ZERO);
            mgr.replicateOnce();
            assertThat(leader.highWatermark()).isEqualTo(new Offset(2));
        }
    }

    @Test
    void no_progress_returns_zero(@TempDir Path tmp) throws IOException {
        PartitionId pid = new PartitionId(new Topic("t"), 0);
        try (Log leaderLog = new Log(pid, tmp.resolve("leader"), 64 * 1024, Long.MAX_VALUE, 128);
             Log followerLog = new Log(pid, tmp.resolve("follower"), 64 * 1024, Long.MAX_VALUE, 128)) {

            PartitionReplica leader = new PartitionReplica(
                pid, 1, leaderLog,
                PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            PartitionReplica follower = new PartitionReplica(
                pid, 2, followerLog,
                PartitionReplica.Role.FOLLOWER, LeaderEpoch.INITIAL,
                Set.of(1, 2), 30_000
            );
            ReplicationManager mgr = new ReplicationManager(follower, leader, 4096);
            assertThat(mgr.replicateOnce()).isZero();
        }
    }
}
