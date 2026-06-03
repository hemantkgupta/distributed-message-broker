package com.hkg.broker.simulator;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Topic;
import com.hkg.broker.metadata.MetadataController;
import com.hkg.broker.replica.PartitionReplica;
import com.hkg.broker.replica.ReplicationManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * High-level harness that wires brokers + a metadata controller + a topic
 * with N partitions across them, returning leader/follower replicas and
 * pre-configured replication managers ready for tests to drive.
 */
public final class ClusterHarness implements AutoCloseable {

    private final Path dataDir;
    private final List<SimulatedBroker> brokers = new ArrayList<>();
    private final MetadataController metadataController = new MetadataController();
    private final Map<PartitionId, PartitionReplica> leaders = new HashMap<>();
    private final Map<PartitionId, List<PartitionReplica>> followers = new HashMap<>();
    private final Map<PartitionId, Map<Integer, PartitionReplica>> replicasByPartition = new HashMap<>();
    private final List<ReplicationManager> replicationManagers = new ArrayList<>();

    public ClusterHarness(Path dataDir) {
        this.dataDir = dataDir;
    }

    public ClusterHarness withBrokers(int n) {
        for (int i = 0; i < n; i++) {
            int id = i + 1;
            brokers.add(new SimulatedBroker(id, dataDir));
            metadataController.registerBroker(id, "host" + id, 9092, "rack" + id);
        }
        return this;
    }

    public ClusterHarness createTopic(String name, int partitionCount, int replicationFactor,
                                      long replicaLagTimeMaxMs) throws IOException {
        metadataController.createTopic(name, partitionCount, replicationFactor);
        Topic topic = new Topic(name);
        for (int p = 0; p < partitionCount; p++) {
            PartitionId pid = new PartitionId(topic, p);
            MetadataController.PartitionState state = metadataController.partitions().get(pid);
            Set<Integer> isr = new HashSet<>(state.replicas());
            // Wire up leader + followers from the harness's broker list.
            SimulatedBroker leaderBroker = brokerById(state.leader());
            PartitionReplica leader = leaderBroker.hostReplica(
                pid, PartitionReplica.Role.LEADER, LeaderEpoch.INITIAL, isr, replicaLagTimeMaxMs);
            leaders.put(pid, leader);
            replicasByPartition.computeIfAbsent(pid, ignored -> new HashMap<>()).put(state.leader(), leader);
            List<PartitionReplica> partitionFollowers = new ArrayList<>();
            for (int replicaId : state.replicas()) {
                if (replicaId == state.leader()) continue;
                SimulatedBroker followerBroker = brokerById(replicaId);
                PartitionReplica follower = followerBroker.hostReplica(
                    pid, PartitionReplica.Role.FOLLOWER, LeaderEpoch.INITIAL, isr, replicaLagTimeMaxMs);
                partitionFollowers.add(follower);
                replicasByPartition.computeIfAbsent(pid, ignored -> new HashMap<>()).put(replicaId, follower);
                replicationManagers.add(new ReplicationManager(follower, leader, /* fetchMax */ 16 * 1024));
            }
            followers.put(pid, partitionFollowers);
        }
        return this;
    }

    private SimulatedBroker brokerById(int id) {
        for (SimulatedBroker b : brokers) {
            if (b.brokerId() == id) return b;
        }
        throw new IllegalStateException("no broker with id " + id);
    }

    /** Drive every follower-side replication once. Returns total batches replicated. */
    public int replicateOnce() throws IOException {
        int total = 0;
        for (ReplicationManager mgr : replicationManagers) {
            total += mgr.replicateOnce();
        }
        return total;
    }

    /** Run replication until quiescent. */
    public int catchUp() throws IOException {
        int total = 0;
        while (true) {
            int n = replicateOnce();
            if (n == 0) return total;
            total += n;
        }
    }

    public PartitionReplica leaderOf(PartitionId partition) {
        return leaders.get(partition);
    }

    public List<PartitionReplica> followersOf(PartitionId partition) {
        return List.copyOf(followers.getOrDefault(partition, List.of()));
    }

    public Map<PartitionId, PartitionReplica> leaders() {
        return Map.copyOf(leaders);
    }

    public Map<PartitionId, Map<Integer, PartitionReplica>> replicasByPartition() {
        Map<PartitionId, Map<Integer, PartitionReplica>> out = new HashMap<>();
        for (Map.Entry<PartitionId, Map<Integer, PartitionReplica>> e : replicasByPartition.entrySet()) {
            out.put(e.getKey(), Map.copyOf(e.getValue()));
        }
        return Map.copyOf(out);
    }

    public PartitionReplica preferredFetchReplica(PartitionId partition, String clientRack) {
        int brokerId = metadataController.closestReplica(partition, clientRack);
        PartitionReplica replica = replicasByPartition.getOrDefault(partition, Map.of()).get(brokerId);
        if (replica == null) {
            throw new IllegalStateException("no replica " + brokerId + " for " + partition);
        }
        return replica;
    }

    public MetadataController metadataController() {
        return metadataController;
    }

    @Override
    public void close() throws IOException {
        for (SimulatedBroker b : brokers) {
            b.shutdown();
        }
    }
}
