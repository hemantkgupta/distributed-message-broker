package com.hkg.broker.metadata;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Simplified KRaft-style metadata controller.
 *
 * <p>The active controller is the sole appender to the {@link MetadataLog};
 * broker registrations, topic creations, and partition leadership changes
 * all flow through it. Brokers themselves are passive observers — they
 * tail the log to learn cluster state.
 *
 * <p>This implementation runs in a single JVM (no real Raft consensus). It
 * still demonstrates the load-bearing structural properties:
 *
 * <ul>
 *   <li>Only the controller mutates state</li>
 *   <li>Brokers learn cluster state by tailing the metadata log, not by
 *       direct RPC to the controller</li>
 *   <li>Leadership changes bump the partition leader epoch monotonically</li>
 * </ul>
 */
public final class MetadataController {

    private final MetadataLog log = new MetadataLog();
    private final Map<Integer, BrokerInfo> brokers = new LinkedHashMap<>();
    private final Map<String, TopicInfo> topics = new HashMap<>();
    private final Map<PartitionId, PartitionState> partitions = new LinkedHashMap<>();

    public synchronized long registerBroker(int brokerId, String host, int port, String rack) {
        BrokerInfo info = new BrokerInfo(brokerId, host, port, rack, false);
        brokers.put(brokerId, info);
        return log.append(new MetadataRecord.RegisterBroker(brokerId, host, port, rack));
    }

    public synchronized long unregisterBroker(int brokerId) {
        brokers.remove(brokerId);
        return log.append(new MetadataRecord.UnregisterBroker(brokerId));
    }

    public synchronized long fenceBroker(int brokerId, long epoch) {
        BrokerInfo info = brokers.get(brokerId);
        if (info != null) {
            brokers.put(brokerId, new BrokerInfo(info.id(), info.host(), info.port(), info.rack(), true));
        }
        return log.append(new MetadataRecord.FenceBroker(brokerId, epoch));
    }

    public synchronized long createTopic(String topicName, int partitionCount, int replicationFactor) {
        if (topics.containsKey(topicName)) {
            throw new IllegalStateException("topic already exists: " + topicName);
        }
        if (brokers.size() < replicationFactor) {
            throw new IllegalStateException(
                "not enough brokers: have " + brokers.size() + ", need " + replicationFactor);
        }
        Topic topic = new Topic(topicName);
        topics.put(topicName, new TopicInfo(topicName, partitionCount, replicationFactor));
        long lastOffset = log.append(new MetadataRecord.CreateTopic(topicName, partitionCount, replicationFactor));
        // Round-robin replica placement.
        List<Integer> brokerIds = new ArrayList<>(brokers.keySet());
        for (int p = 0; p < partitionCount; p++) {
            List<Integer> replicas = new ArrayList<>();
            for (int r = 0; r < replicationFactor; r++) {
                replicas.add(brokerIds.get((p + r) % brokerIds.size()));
            }
            PartitionId pid = new PartitionId(topic, p);
            int leader = replicas.get(0);
            PartitionState state = new PartitionState(
                pid, leader, LeaderEpoch.INITIAL, List.copyOf(replicas), List.copyOf(replicas));
            partitions.put(pid, state);
            lastOffset = log.append(new MetadataRecord.PartitionLeadership(
                pid, leader, LeaderEpoch.INITIAL, state.replicas(), state.isr()));
        }
        return lastOffset;
    }

    /**
     * Move leadership for the partition to a new broker. Bumps the leader
     * epoch and emits a metadata record so brokers tailing the log learn
     * about it.
     */
    public synchronized long changeLeader(PartitionId partition, int newLeader) {
        PartitionState current = partitions.get(partition);
        if (current == null) {
            throw new IllegalStateException("unknown partition " + partition);
        }
        if (!current.replicas().contains(newLeader)) {
            throw new IllegalArgumentException(
                "new leader " + newLeader + " is not in replica set " + current.replicas());
        }
        LeaderEpoch nextEpoch = current.leaderEpoch().next();
        PartitionState next = new PartitionState(
            partition, newLeader, nextEpoch, current.replicas(), current.isr());
        partitions.put(partition, next);
        return log.append(new MetadataRecord.PartitionLeadership(
            partition, newLeader, nextEpoch, current.replicas(), current.isr()));
    }

    /** Update the ISR. Always emitted as a new metadata record. */
    public synchronized long updateIsr(PartitionId partition, List<Integer> newIsr) {
        PartitionState current = partitions.get(partition);
        if (current == null) {
            throw new IllegalStateException("unknown partition " + partition);
        }
        PartitionState next = new PartitionState(
            partition, current.leader(), current.leaderEpoch(), current.replicas(), List.copyOf(newIsr));
        partitions.put(partition, next);
        return log.append(new MetadataRecord.PartitionLeadership(
            partition, current.leader(), current.leaderEpoch(), current.replicas(), next.isr()));
    }

    public synchronized MetadataLog metadataLog() { return log; }
    public synchronized Map<Integer, BrokerInfo> brokers() { return Map.copyOf(brokers); }
    public synchronized Map<PartitionId, PartitionState> partitions() { return Map.copyOf(partitions); }

    /**
     * KIP-392-style closest-replica selection: prefer an in-sync replica in
     * the client's rack, otherwise fall back to the leader.
     */
    public synchronized int closestReplica(PartitionId partition, String clientRack) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            throw new IllegalStateException("unknown partition " + partition);
        }
        if (clientRack != null) {
            for (int replicaId : state.isr()) {
                BrokerInfo broker = brokers.get(replicaId);
                if (broker != null && clientRack.equals(broker.rack())) {
                    return replicaId;
                }
            }
        }
        return state.leader();
    }

    public record BrokerInfo(int id, String host, int port, String rack, boolean fenced) {
        public BrokerInfo {
            Objects.requireNonNull(host);
            Objects.requireNonNull(rack);
        }
    }

    public record TopicInfo(String name, int partitionCount, int replicationFactor) {
        public TopicInfo {
            Objects.requireNonNull(name);
            if (partitionCount <= 0 || replicationFactor <= 0) {
                throw new IllegalArgumentException("partitionCount and replicationFactor must be positive");
            }
        }
    }

    public record PartitionState(
        PartitionId partition, int leader, LeaderEpoch leaderEpoch,
        List<Integer> replicas, List<Integer> isr
    ) {}
}
