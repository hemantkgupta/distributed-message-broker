package com.hkg.broker.coordinator;

import com.hkg.broker.common.PartitionId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Consumer-group coordinator state machine.
 *
 * <p>Implements the protocol shape:
 * <pre>
 *   JoinGroup → coordinator picks a leader, group transitions to PREPARING
 *               (Note: this implementation is single-leader; the first
 *                joiner is always the assignor.)
 *   SyncGroup → leader submits the assignment, coordinator distributes
 *               it back to members; group transitions to STABLE
 *   Heartbeat → keeps a member alive
 *   LeaveGroup → triggers a rebalance
 * </pre>
 *
 * <p>The default assignor is range-style: partitions are split evenly
 * across members in member-id sorted order. Cooperative incremental
 * rebalancing (KIP-429) is documented in the wiki implementation page as
 * roadmap.
 *
 * <p>Static membership (KIP-345) is supported via the optional
 * {@code groupInstanceId}: if a join request arrives with a matching
 * existing static member, no rebalance is triggered; the existing
 * assignment is returned directly.
 */
public final class ConsumerGroupCoordinator {

    public enum State { EMPTY, PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE }

    private final String groupId;
    private final long sessionTimeoutMs;
    private State state = State.EMPTY;
    private int generation = 0;
    private String leaderId;
    private final Map<String, Member> members = new LinkedHashMap<>();
    private Map<String, List<PartitionId>> assignment = new HashMap<>();

    public ConsumerGroupCoordinator(String groupId, long sessionTimeoutMs) {
        this.groupId = Objects.requireNonNull(groupId);
        if (sessionTimeoutMs <= 0) {
            throw new IllegalArgumentException("sessionTimeoutMs must be positive");
        }
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public synchronized JoinResult joinGroup(String memberId, String groupInstanceId,
                                             List<PartitionId> requestedPartitions,
                                             long nowMs) {
        Member existing = members.get(memberId);
        boolean isStaticReconnect = false;
        if (groupInstanceId != null) {
            for (Member m : members.values()) {
                if (groupInstanceId.equals(m.groupInstanceId())) {
                    existing = m;
                    isStaticReconnect = true;
                    break;
                }
            }
        }
        if (existing != null && isStaticReconnect && state == State.STABLE) {
            // KIP-345 static membership: silent reconnect, no rebalance.
            existing.touch(nowMs);
            return new JoinResult(generation, existing.id(), leaderId,
                assignment.getOrDefault(existing.id(), List.of()));
        }
        Member m = new Member(memberId, groupInstanceId, requestedPartitions, nowMs);
        members.put(memberId, m);
        triggerRebalance();
        // First joiner becomes the group leader.
        if (leaderId == null) leaderId = memberId;
        // Eager protocol: members get an empty assignment until SyncGroup.
        return new JoinResult(generation, memberId, leaderId, List.of());
    }

    public synchronized SyncResult syncGroup(String memberId, Map<String, List<PartitionId>> proposedAssignment) {
        if (state != State.PREPARING_REBALANCE && state != State.COMPLETING_REBALANCE) {
            throw new IllegalStateException("sync only valid during rebalance, state=" + state);
        }
        if (!Objects.equals(memberId, leaderId)) {
            throw new IllegalStateException("only the group leader may submit assignments");
        }
        this.assignment = new HashMap<>();
        for (Map.Entry<String, List<PartitionId>> e : proposedAssignment.entrySet()) {
            if (!members.containsKey(e.getKey())) {
                throw new IllegalArgumentException("assignment references unknown member " + e.getKey());
            }
            this.assignment.put(e.getKey(), List.copyOf(e.getValue()));
        }
        for (String id : members.keySet()) {
            assignment.putIfAbsent(id, List.of());
        }
        state = State.STABLE;
        return new SyncResult(generation, assignment);
    }

    /**
     * Returns a default range-style assignment for the supplied partitions,
     * for use by the group leader during SyncGroup. Members are sorted by
     * id for stability.
     */
    public synchronized Map<String, List<PartitionId>> defaultAssign(List<PartitionId> partitions) {
        List<String> ids = new ArrayList<>(members.keySet());
        Collections.sort(ids);
        Map<String, List<PartitionId>> out = new HashMap<>();
        for (String id : ids) out.put(id, new ArrayList<>());
        if (ids.isEmpty()) return out;
        int n = partitions.size();
        int m = ids.size();
        int basePer = n / m;
        int remainder = n % m;
        int cursor = 0;
        for (int i = 0; i < m; i++) {
            int take = basePer + (i < remainder ? 1 : 0);
            for (int j = 0; j < take && cursor < n; j++) {
                out.get(ids.get(i)).add(partitions.get(cursor++));
            }
        }
        return out;
    }

    public synchronized List<PartitionId> assignmentFor(String memberId) {
        return assignment.getOrDefault(memberId, List.of());
    }

    public synchronized void heartbeat(String memberId, long nowMs) {
        Member m = members.get(memberId);
        if (m == null) {
            throw new IllegalStateException("unknown member " + memberId);
        }
        m.touch(nowMs);
    }

    public synchronized void leaveGroup(String memberId) {
        Member removed = members.remove(memberId);
        if (removed == null) return;
        if (Objects.equals(memberId, leaderId)) {
            leaderId = members.isEmpty() ? null : members.keySet().iterator().next();
        }
        assignment.remove(memberId);
        if (members.isEmpty()) {
            state = State.EMPTY;
        } else {
            triggerRebalance();
        }
    }

    /**
     * Evict members whose last heartbeat is older than the session timeout.
     * Returns the evicted member IDs.
     */
    public synchronized Set<String> evictExpired(long nowMs) {
        Set<String> evicted = new HashSet<>();
        for (String id : new ArrayList<>(members.keySet())) {
            Member m = members.get(id);
            if (nowMs - m.lastHeartbeatMs() > sessionTimeoutMs) {
                members.remove(id);
                assignment.remove(id);
                evicted.add(id);
                if (Objects.equals(id, leaderId)) {
                    leaderId = members.isEmpty() ? null : members.keySet().iterator().next();
                }
            }
        }
        if (!evicted.isEmpty() && !members.isEmpty()) {
            triggerRebalance();
        } else if (members.isEmpty()) {
            state = State.EMPTY;
        }
        return evicted;
    }

    private void triggerRebalance() {
        state = State.PREPARING_REBALANCE;
        generation++;
        assignment = new HashMap<>();
    }

    public synchronized State state() { return state; }
    public synchronized int generation() { return generation; }
    public synchronized String leaderId() { return leaderId; }
    public synchronized Set<String> memberIds() { return Set.copyOf(members.keySet()); }
    public String groupId() { return groupId; }

    public record JoinResult(int generation, String memberId, String leaderId, List<PartitionId> assignment) {}
    public record SyncResult(int generation, Map<String, List<PartitionId>> assignment) {}

    private static final class Member {
        private final String id;
        private final String groupInstanceId;
        private final List<PartitionId> requestedPartitions;
        private long lastHeartbeatMs;

        Member(String id, String groupInstanceId, List<PartitionId> requestedPartitions, long nowMs) {
            this.id = id;
            this.groupInstanceId = groupInstanceId;
            this.requestedPartitions = List.copyOf(requestedPartitions);
            this.lastHeartbeatMs = nowMs;
        }

        String id() { return id; }
        String groupInstanceId() { return groupInstanceId; }
        long lastHeartbeatMs() { return lastHeartbeatMs; }
        void touch(long nowMs) { lastHeartbeatMs = nowMs; }
    }
}
