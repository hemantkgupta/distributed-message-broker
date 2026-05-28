package com.hkg.broker.simulator;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.replica.PartitionReplica;
import com.hkg.broker.storage.Log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * One in-process broker. Owns a fixed number of partition replicas (each
 * with its own on-disk Log). A real broker would also host a network
 * server, group coordinator, and metadata observer thread — those concerns
 * are wired separately by the simulator harness.
 */
public final class SimulatedBroker {

    private final int brokerId;
    private final Path dataDir;
    private final Map<PartitionId, PartitionReplica> replicas = new HashMap<>();

    public SimulatedBroker(int brokerId, Path dataDir) {
        this.brokerId = brokerId;
        this.dataDir = dataDir;
    }

    public synchronized PartitionReplica hostReplica(
        PartitionId partition, PartitionReplica.Role role,
        LeaderEpoch initialEpoch, java.util.Set<Integer> isr,
        long replicaLagTimeMaxMs
    ) throws IOException {
        Path partitionDir = dataDir.resolve("b" + brokerId).resolve(partition.toString());
        Log log = new Log(partition, partitionDir,
            /* segmentBytes */ 16 * 1024,
            /* segmentMs */ Long.MAX_VALUE,
            /* indexInterval */ 256);
        PartitionReplica replica = new PartitionReplica(
            partition, brokerId, log, role, initialEpoch, isr, replicaLagTimeMaxMs);
        replicas.put(partition, replica);
        return replica;
    }

    public synchronized PartitionReplica replicaOf(PartitionId partition) {
        return replicas.get(partition);
    }

    public int brokerId() {
        return brokerId;
    }

    /** Close all hosted replicas' underlying logs. */
    public synchronized void shutdown() throws IOException {
        for (PartitionReplica r : replicas.values()) {
            r.log().close();
        }
        replicas.clear();
    }
}
