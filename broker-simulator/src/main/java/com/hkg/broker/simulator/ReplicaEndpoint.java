package com.hkg.broker.simulator;

import com.hkg.broker.client.BrokerEndpoint;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;
import com.hkg.broker.metadata.MetadataController;
import com.hkg.broker.replica.PartitionReplica;
import com.hkg.broker.storage.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * In-process {@link BrokerEndpoint} that delegates to a map of
 * {@link PartitionReplica} instances — typically the cluster's leaders.
 *
 * <p>This is what makes the {@code broker-client} talk to actual replica
 * state machines without going over a real socket. Phase 5's e2e tests
 * use this to drive end-to-end produce/fetch through the full ISR + HWM
 * pipeline.
 */
public final class ReplicaEndpoint implements BrokerEndpoint {

    private final Map<PartitionId, PartitionReplica> leaders;
    private final Map<PartitionId, Map<Integer, PartitionReplica>> replicasByPartition;
    private final MetadataController metadataController;
    private final String clientRack;
    private final NetworkPartitionFilter filter;

    public ReplicaEndpoint(Map<PartitionId, PartitionReplica> leaders) {
        this(leaders, new NetworkPartitionFilter());
    }

    public ReplicaEndpoint(Map<PartitionId, PartitionReplica> leaders, NetworkPartitionFilter filter) {
        this.leaders = leaders;
        this.replicasByPartition = Map.of();
        this.metadataController = null;
        this.clientRack = null;
        this.filter = filter;
    }

    public ReplicaEndpoint(
        Map<PartitionId, PartitionReplica> leaders,
        Map<PartitionId, Map<Integer, PartitionReplica>> replicasByPartition,
        MetadataController metadataController,
        String clientRack
    ) {
        this.leaders = leaders;
        this.replicasByPartition = replicasByPartition;
        this.metadataController = metadataController;
        this.clientRack = clientRack;
        this.filter = new NetworkPartitionFilter();
    }

    @Override
    public ProduceResult produce(PartitionId partition, List<Record> records) throws IOException {
        if (filter.isPartitionUnreachable(partition)) {
            throw new IOException("simulated network partition: " + partition);
        }
        PartitionReplica leader = leaders.get(partition);
        if (leader == null) {
            throw new IOException("no leader endpoint for " + partition);
        }
        Log.AppendResult result = leader.produce(records);
        return new ProduceResult(result.baseOffset(), result.lastOffset());
    }

    @Override
    public List<RecordBatch> fetch(PartitionId partition, Offset from, int maxBytes) throws IOException {
        if (filter.isPartitionUnreachable(partition)) {
            throw new IOException("simulated network partition: " + partition);
        }
        PartitionReplica replica = fetchReplica(partition);
        if (replica == null) {
            throw new IOException("no fetch endpoint for " + partition);
        }
        return replica.fetch(from, maxBytes);
    }

    public NetworkPartitionFilter filter() {
        return filter;
    }

    private PartitionReplica fetchReplica(PartitionId partition) {
        if (metadataController == null) {
            return leaders.get(partition);
        }
        int brokerId = metadataController.closestReplica(partition, clientRack);
        return replicasByPartition.getOrDefault(partition, Map.of()).get(brokerId);
    }
}
