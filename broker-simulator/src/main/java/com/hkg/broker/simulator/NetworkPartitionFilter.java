package com.hkg.broker.simulator;

import com.hkg.broker.common.PartitionId;

import java.util.HashSet;
import java.util.Set;

/**
 * Chaos primitive: marks selected partitions as unreachable for the
 * {@link ReplicaEndpoint}, simulating a network partition between the
 * client and the leader broker. Used in Phase 5 e2e tests to verify the
 * client's failure handling.
 */
public final class NetworkPartitionFilter {

    private final Set<PartitionId> unreachable = new HashSet<>();

    public synchronized void cut(PartitionId partition) {
        unreachable.add(partition);
    }

    public synchronized void heal(PartitionId partition) {
        unreachable.remove(partition);
    }

    public synchronized void healAll() {
        unreachable.clear();
    }

    public synchronized boolean isPartitionUnreachable(PartitionId partition) {
        return unreachable.contains(partition);
    }
}
