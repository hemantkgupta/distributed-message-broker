package com.hkg.broker.common;

import java.util.Objects;

/** Identifier for one partition of a topic. */
public record PartitionId(Topic topic, int partition) {

    public PartitionId {
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition < 0: " + partition);
        }
    }

    @Override
    public String toString() {
        return topic.name() + "-" + partition;
    }
}
