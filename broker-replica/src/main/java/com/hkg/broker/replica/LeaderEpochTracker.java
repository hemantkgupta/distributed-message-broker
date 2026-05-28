package com.hkg.broker.replica;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * KIP-101 per-partition leader-epoch cache.
 *
 * <p>Holds an ordered list of {@code (epoch, startOffset)} entries — one per
 * leadership tenure. On leader handoff, a follower issues an
 * {@code OffsetsForLeaderEpoch} request to the new leader; the leader looks
 * up the end offset of the requested epoch (the start offset of the next
 * epoch, or its current log end offset if there is no next epoch) and the
 * follower truncates its local log to that point before replicating forward.
 *
 * <p>The cache only grows on leadership changes, so its size is bounded by
 * leadership-change frequency rather than by write rate.
 */
public final class LeaderEpochTracker {

    private final List<Entry> entries = new ArrayList<>();

    /** Add a new leadership tenure. Epochs must strictly increase. */
    public synchronized void recordLeadershipStart(LeaderEpoch epoch, Offset startOffset) {
        if (!entries.isEmpty()) {
            Entry tail = entries.get(entries.size() - 1);
            if (epoch.compareTo(tail.epoch()) <= 0) {
                throw new IllegalArgumentException(
                    "epoch " + epoch + " must be strictly greater than " + tail.epoch());
            }
            if (startOffset.compareTo(tail.startOffset()) < 0) {
                throw new IllegalArgumentException(
                    "startOffset " + startOffset + " must be at least " + tail.startOffset());
            }
        }
        entries.add(new Entry(epoch, startOffset));
    }

    /**
     * Return the end offset of the given epoch, defined as the start offset
     * of the immediately following epoch. If the queried epoch is the latest
     * one, returns the supplied {@code logEndOffset}. If the epoch is not in
     * the cache at all, returns empty.
     */
    public synchronized Optional<Offset> endOffsetOf(LeaderEpoch epoch, Offset logEndOffset) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).epoch().equals(epoch)) {
                if (i == entries.size() - 1) {
                    return Optional.of(logEndOffset);
                }
                return Optional.of(entries.get(i + 1).startOffset());
            }
        }
        return Optional.empty();
    }

    /** Latest known epoch (or empty if none recorded yet). */
    public synchronized Optional<LeaderEpoch> latestEpoch() {
        if (entries.isEmpty()) return Optional.empty();
        return Optional.of(entries.get(entries.size() - 1).epoch());
    }

    /** Defensive snapshot. */
    public synchronized List<Entry> snapshot() {
        return List.copyOf(entries);
    }

    /**
     * Truncate the cache to remove epochs strictly greater than {@code epoch}.
     * Used during follower recovery when divergence is detected.
     */
    public synchronized void truncateToEpoch(LeaderEpoch epoch) {
        entries.removeIf(e -> e.epoch().compareTo(epoch) > 0);
    }

    public record Entry(LeaderEpoch epoch, Offset startOffset) {}
}
