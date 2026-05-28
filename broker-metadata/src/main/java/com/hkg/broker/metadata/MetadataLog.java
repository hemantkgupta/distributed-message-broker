package com.hkg.broker.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-process stand-in for the {@code __cluster_metadata} replicated log.
 *
 * <p>Each entry has a monotonically increasing offset. Brokers can {@link
 * #tailFrom(long)} to receive records since their last applied offset, the
 * same way they pull from {@code __cluster_metadata} in a real KRaft
 * cluster. The controller is the only writer; the log itself is an
 * append-only sequence.
 */
public final class MetadataLog {

    private final List<Entry> entries = new ArrayList<>();

    /** Append a record; returns the assigned offset. */
    public synchronized long append(MetadataRecord record) {
        long offset = entries.size();
        entries.add(new Entry(offset, record));
        return offset;
    }

    /** Total number of records appended so far. */
    public synchronized long highWatermark() {
        return entries.size();
    }

    /** Return every record at offset >= {@code fromOffset}. */
    public synchronized List<Entry> tailFrom(long fromOffset) {
        if (fromOffset >= entries.size()) return List.of();
        return Collections.unmodifiableList(new ArrayList<>(entries.subList((int) fromOffset, entries.size())));
    }

    /** Total record count. */
    public synchronized int size() {
        return entries.size();
    }

    public record Entry(long offset, MetadataRecord record) {}
}
