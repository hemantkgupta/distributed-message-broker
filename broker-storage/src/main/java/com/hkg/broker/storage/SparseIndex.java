package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;

import java.util.ArrayList;
import java.util.List;

/**
 * Sparse offset-to-position index for a single log segment.
 *
 * <p>An entry is added every {@code indexIntervalBytes} of segment data, so the
 * memory footprint is bounded by segment-size / interval rather than by record
 * count. Lookup is binary search to the nearest indexed offset at or below
 * the target, then the caller does a sequential scan of the log file from that
 * physical position to the target record.
 *
 * <p>This is the same mechanism Apache Kafka uses; the default interval there is
 * 4 KiB. We expose it as a constructor parameter so tests can use coarser
 * intervals without enormous fixture data.
 */
public final class SparseIndex {

    private final int indexIntervalBytes;
    private final List<Entry> entries = new ArrayList<>();
    private long bytesSinceLastIndex;

    public SparseIndex(int indexIntervalBytes) {
        if (indexIntervalBytes <= 0) {
            throw new IllegalArgumentException("indexIntervalBytes must be positive");
        }
        this.indexIntervalBytes = indexIntervalBytes;
    }

    /**
     * Notify the index that a batch starting at the given offset was written
     * at the given physical position. The index decides whether to record an
     * entry based on bytes-since-last-index.
     */
    public void maybeAppend(Offset offset, long position, int batchSizeBytes) {
        if (entries.isEmpty() || bytesSinceLastIndex >= indexIntervalBytes) {
            entries.add(new Entry(offset, position));
            bytesSinceLastIndex = 0;
        }
        bytesSinceLastIndex += batchSizeBytes;
    }

    /**
     * Return the physical position of the indexed offset ≤ {@code target}.
     * The caller must scan forward from that position to find the exact batch
     * containing {@code target}.
     *
     * <p>Returns -1 if the index is empty.
     */
    public long lookupFloor(Offset target) {
        if (entries.isEmpty()) return -1L;
        int lo = 0;
        int hi = entries.size() - 1;
        long answer = -1L;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            Entry e = entries.get(mid);
            int cmp = e.offset().compareTo(target);
            if (cmp <= 0) {
                answer = e.position();
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return answer;
    }

    public int size() {
        return entries.size();
    }

    public record Entry(Offset offset, long position) {}
}
