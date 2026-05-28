package com.hkg.broker.common;

/**
 * KIP-101 leader epoch. Monotonically increasing per partition; bumped on every
 * leadership change. Stamped on every batch the leader writes so followers can
 * truncate cleanly on leader handoff.
 */
public record LeaderEpoch(int value) implements Comparable<LeaderEpoch> {

    public static final LeaderEpoch INITIAL = new LeaderEpoch(0);

    public LeaderEpoch {
        if (value < 0) {
            throw new IllegalArgumentException("epoch < 0: " + value);
        }
    }

    public LeaderEpoch next() {
        return new LeaderEpoch(value + 1);
    }

    @Override
    public int compareTo(LeaderEpoch other) {
        return Integer.compare(this.value, other.value);
    }
}
