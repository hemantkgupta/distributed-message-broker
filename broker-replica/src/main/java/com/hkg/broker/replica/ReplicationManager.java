package com.hkg.broker.replica;

import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.util.List;

/**
 * Drives the follower side of partition replication.
 *
 * <p>For each partition this broker hosts as a follower, the manager
 * periodically issues a replica-fetch against the current leader and
 * applies the returned batches to the local log. This implementation
 * exposes a step function rather than a background thread so tests can
 * deterministically advance replication one fetch at a time.
 */
public final class ReplicationManager {

    private final PartitionReplica follower;
    private final PartitionReplica leader;
    private final int maxFetchBytes;

    public ReplicationManager(PartitionReplica follower, PartitionReplica leader, int maxFetchBytes) {
        if (follower.role() != PartitionReplica.Role.FOLLOWER) {
            throw new IllegalArgumentException("follower replica is not in FOLLOWER role");
        }
        if (leader.role() != PartitionReplica.Role.LEADER) {
            throw new IllegalArgumentException("leader replica is not in LEADER role");
        }
        this.follower = follower;
        this.leader = leader;
        this.maxFetchBytes = maxFetchBytes;
    }

    /**
     * Single pass: ask the leader for batches starting at our LEO; apply
     * them locally. Returns the number of batches replicated.
     */
    public int replicateOnce() throws IOException {
        var followerLEO = follower.logEndOffset();
        List<RecordBatch> batches = leader.replicaFetch(follower.brokerId(), followerLEO, maxFetchBytes);
        int n = 0;
        for (RecordBatch b : batches) {
            follower.applyFromLeader(b);
            n++;
        }
        // Refresh the leader's view of our LEO now that we've applied.
        leader.onFollowerFetch(follower.brokerId(), follower.logEndOffset());
        return n;
    }

    /** Run replication until no new batches arrive. */
    public int catchUp() throws IOException {
        int total = 0;
        while (true) {
            int n = replicateOnce();
            if (n == 0) return total;
            total += n;
        }
    }
}
