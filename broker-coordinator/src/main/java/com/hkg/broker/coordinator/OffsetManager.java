package com.hkg.broker.coordinator;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory stand-in for Kafka's {@code __consumer_offsets} compacted topic.
 *
 * <p>Stores per-(group, partition) commit records. In a production broker
 * these would be appended to {@code __consumer_offsets} (a compacted topic
 * keyed by {@code group:topic:partition}); here the same shape lives in a
 * map so tests can drive it without booting the full storage stack.
 *
 * <p>The carried fields match Kafka's commit-record value schema:
 * {@code (offset, leaderEpoch, commitTimestamp, metadata)}.
 */
public final class OffsetManager {

    private final Map<Key, CommittedOffset> commits = new HashMap<>();

    public synchronized void commit(String group, PartitionId partition,
                                    Offset offset, LeaderEpoch leaderEpoch,
                                    long commitTimestampMs, String metadata) {
        commits.put(new Key(group, partition),
            new CommittedOffset(offset, leaderEpoch, commitTimestampMs, metadata));
    }

    public synchronized Optional<CommittedOffset> fetch(String group, PartitionId partition) {
        return Optional.ofNullable(commits.get(new Key(group, partition)));
    }

    public synchronized int size() {
        return commits.size();
    }

    public synchronized void delete(String group, PartitionId partition) {
        commits.remove(new Key(group, partition));
    }

    public record CommittedOffset(Offset offset, LeaderEpoch leaderEpoch, long commitTimestampMs, String metadata) {}

    private record Key(String group, PartitionId partition) {}
}
