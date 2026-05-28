package com.hkg.broker.metadata;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.PartitionId;

import java.util.List;

/**
 * Sealed hierarchy of records that appear in the {@code __cluster_metadata}
 * log. In Apache Kafka these are full Protobuf-style schemas; this version
 * keeps the shape (RegisterBroker, BrokerRegistrationChange, Partition,
 * FenceBroker) for the load-bearing record types and skips the rest.
 */
public sealed interface MetadataRecord {

    record RegisterBroker(int brokerId, String host, int port, String rack) implements MetadataRecord {}

    record UnregisterBroker(int brokerId) implements MetadataRecord {}

    record FenceBroker(int brokerId, long epoch) implements MetadataRecord {}

    record CreateTopic(String topic, int partitionCount, int replicationFactor) implements MetadataRecord {}

    /**
     * Partition leadership state. {@code replicas} is the static replica
     * set; {@code isr} is the current in-sync subset; {@code leader} is the
     * broker currently serving as partition leader (or -1 if none).
     */
    record PartitionLeadership(PartitionId partition, int leader, LeaderEpoch leaderEpoch,
                               List<Integer> replicas, List<Integer> isr) implements MetadataRecord {}
}
