package com.hkg.broker.replica;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;
import com.hkg.broker.storage.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One partition replica running on one broker. Tracks the role (leader vs
 * follower) and exposes the produce + fetch path.
 *
 * <p>The leader maintains the canonical ISR + high watermark. Followers
 * replicate by issuing fetch requests against the leader; their progress
 * advances the leader's per-follower {@code logEndOffset} (LEO) tracking,
 * which drives ISR shrink/expand and HWM advancement.
 *
 * <p>The HWM is computed as {@code min(LEO across all in-sync members of
 * the ISR)} and bounds what consumers can read — the {@link #fetch}
 * method never returns batches whose last offset exceeds the HWM.
 */
public final class PartitionReplica {

    public enum Role { LEADER, FOLLOWER }

    private final PartitionId partitionId;
    private final int brokerId;
    private final Log log;
    private final LeaderEpochTracker epochTracker;

    private Role role;
    private LeaderEpoch currentEpoch;
    private Set<Integer> isr;
    private final Map<Integer, Offset> followerLEOs = new HashMap<>();
    private final long replicaLagTimeMaxMs;
    private final Map<Integer, Long> followerLastFetchAtMs = new HashMap<>();
    private Offset highWatermark = Offset.ZERO;
    private int minInsyncReplicas = 1;

    public PartitionReplica(
        PartitionId partitionId,
        int brokerId,
        Log log,
        Role role,
        LeaderEpoch initialEpoch,
        Set<Integer> isr,
        long replicaLagTimeMaxMs
    ) {
        this.partitionId = partitionId;
        this.brokerId = brokerId;
        this.log = log;
        this.epochTracker = new LeaderEpochTracker();
        this.role = role;
        this.currentEpoch = initialEpoch;
        this.isr = new HashSet<>(isr);
        this.replicaLagTimeMaxMs = replicaLagTimeMaxMs;
        if (role == Role.LEADER) {
            this.epochTracker.recordLeadershipStart(initialEpoch, log.logEndOffset());
        }
    }

    /**
     * Become leader at the next epoch. Records the leadership transition in
     * the epoch tracker and resets follower-fetch tracking.
     */
    public synchronized void becomeLeader(LeaderEpoch newEpoch, Set<Integer> isr) {
        if (newEpoch.compareTo(this.currentEpoch) <= 0) {
            throw new IllegalArgumentException(
                "new epoch " + newEpoch + " must exceed current " + this.currentEpoch);
        }
        this.role = Role.LEADER;
        this.currentEpoch = newEpoch;
        this.isr = new HashSet<>(isr);
        this.followerLEOs.clear();
        this.followerLastFetchAtMs.clear();
        this.epochTracker.recordLeadershipStart(newEpoch, log.logEndOffset());
        recomputeHwm();
    }

    public synchronized void becomeFollower(LeaderEpoch leaderEpoch) {
        if (leaderEpoch.compareTo(this.currentEpoch) < 0) {
            throw new IllegalArgumentException(
                "follower epoch " + leaderEpoch + " is older than current " + this.currentEpoch);
        }
        this.role = Role.FOLLOWER;
        this.currentEpoch = leaderEpoch;
        this.followerLEOs.clear();
        this.followerLastFetchAtMs.clear();
    }

    public Role role() { return role; }
    public LeaderEpoch currentEpoch() { return currentEpoch; }
    public PartitionId partitionId() { return partitionId; }
    public int brokerId() { return brokerId; }
    public Log log() { return log; }
    public LeaderEpochTracker epochTracker() { return epochTracker; }

    public synchronized Offset highWatermark() { return highWatermark; }
    public synchronized Set<Integer> isr() { return Set.copyOf(isr); }
    public synchronized Offset logEndOffset() { return log.logEndOffset(); }

    public synchronized void updateHighWatermark(Offset highWatermark) {
        if (highWatermark.compareTo(log.logEndOffset()) > 0) {
            throw new IllegalArgumentException("HWM cannot exceed local LEO");
        }
        if (highWatermark.compareTo(this.highWatermark) > 0) {
            this.highWatermark = highWatermark;
        }
    }

    public synchronized void setMinInsyncReplicas(int n) {
        if (n < 1) throw new IllegalArgumentException("min.insync.replicas must be >= 1");
        this.minInsyncReplicas = n;
    }

    /**
     * Leader-side produce. Appends the records, stamping them with the
     * current leader epoch. The HWM is not immediately advanced — followers
     * must fetch the batch first.
     *
     * <p>If the request requires {@code acks=all}, the caller is expected to
     * wait for the HWM to reach the returned offset before acking the
     * producer. That orchestration lives in the network layer's
     * {@code DelayedProduce}; this method only handles the local append.
     */
    public synchronized Log.AppendResult produce(List<Record> records) throws IOException {
        if (role != Role.LEADER) {
            throw new IllegalStateException("not the leader for " + partitionId);
        }
        if (isr.size() < minInsyncReplicas) {
            throw new IllegalStateException(
                "insufficient in-sync replicas: ISR=" + isr.size()
                    + " < min.insync.replicas=" + minInsyncReplicas);
        }
        Log.AppendResult result = log.append(records, currentEpoch);
        // The leader is always "in-sync" with itself, so its LEO matches the log end offset.
        followerLEOs.put(brokerId, log.logEndOffset());
        followerLastFetchAtMs.put(brokerId, System.currentTimeMillis());
        recomputeHwm();
        return result;
    }

    /**
     * Follower-side append. Used when this replica is replaying a batch
     * received from the leader's fetch reply.
     */
    public synchronized Log.AppendResult applyFromLeader(RecordBatch batch) throws IOException {
        if (role != Role.FOLLOWER) {
            throw new IllegalStateException("not a follower for " + partitionId);
        }
        return log.appendBatch(batch);
    }

    /**
     * Bookkeeping called by the leader when a follower issues a fetch.
     * The follower's reported LEO drives ISR membership and HWM advancement.
     */
    public synchronized void onFollowerFetch(int followerId, Offset followerLEO) {
        if (role != Role.LEADER) return;
        followerLEOs.put(followerId, followerLEO);
        followerLastFetchAtMs.put(followerId, System.currentTimeMillis());
        maybeExpandIsr(followerId);
        recomputeHwm();
    }

    /**
     * Remove from the ISR any follower whose last fetch is older than the
     * lag threshold. Called periodically by the broker housekeeping thread
     * (or in tests, directly).
     */
    public synchronized void shrinkIsrForLaggingFollowers(long nowMs) {
        if (role != Role.LEADER) return;
        Set<Integer> next = new HashSet<>(isr);
        for (int id : isr) {
            if (id == brokerId) continue;
            Long lastFetch = followerLastFetchAtMs.get(id);
            if (lastFetch == null) {
                // Never fetched — treat as infinitely lagging.
                next.remove(id);
            } else if ((nowMs - lastFetch) > replicaLagTimeMaxMs) {
                next.remove(id);
            }
        }
        if (!next.contains(brokerId)) next.add(brokerId);
        if (!next.equals(isr)) {
            this.isr = next;
            recomputeHwm();
        }
    }

    private void maybeExpandIsr(int followerId) {
        if (followerId == brokerId) return;
        if (isr.contains(followerId)) return;
        Offset leo = followerLEOs.get(followerId);
        if (leo != null && leo.compareTo(log.logEndOffset()) >= 0) {
            isr.add(followerId);
        }
    }

    private void recomputeHwm() {
        if (role != Role.LEADER || isr.isEmpty()) return;
        Offset min = log.logEndOffset();
        for (int id : isr) {
            Offset leo = id == brokerId ? log.logEndOffset() : followerLEOs.getOrDefault(id, Offset.ZERO);
            if (leo.compareTo(min) < 0) min = leo;
        }
        if (min.compareTo(highWatermark) > 0) {
            highWatermark = min;
        }
    }

    /**
     * Consumer-side fetch: return batches starting at {@code from}, up to
     * {@code maxBytes}, but never beyond the high watermark.
     */
    public synchronized List<RecordBatch> fetch(Offset from, int maxBytes) throws IOException {
        if (from.compareTo(highWatermark) >= 0) {
            return Collections.emptyList();
        }
        List<RecordBatch> all = log.readBatches(from, maxBytes);
        List<RecordBatch> visible = new ArrayList<>();
        for (RecordBatch b : all) {
            if (b.baseOffset().compareTo(highWatermark) >= 0) break;
            visible.add(b);
        }
        return visible;
    }

    /**
     * Replica-side fetch: a follower asking the leader for more data. Returns
     * everything up to the leader's log end offset (NOT bounded by HWM).
     */
    public synchronized List<RecordBatch> replicaFetch(int followerId, Offset followerLEO, int maxBytes) throws IOException {
        if (role != Role.LEADER) {
            throw new IllegalStateException("not the leader for " + partitionId);
        }
        onFollowerFetch(followerId, followerLEO);
        return log.readBatches(followerLEO, maxBytes);
    }
}
