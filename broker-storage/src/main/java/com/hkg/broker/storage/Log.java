package com.hkg.broker.storage;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The partition log. Composed of an active {@link LogSegment} (open for
 * appends) and zero or more sealed segments (read-only). When the active
 * segment crosses {@code segmentBytes} or {@code segmentMillis}, it is
 * sealed and a new active segment opens.
 *
 * <p>Segments are named {@code <baseOffset>.log} with a 20-digit zero-padded
 * offset, matching Kafka's naming convention so segments sort by offset
 * lexicographically.
 *
 * <p>This class is the production-grade boundary between the broker's
 * networking + replica layers and the on-disk format. All offset assignment
 * happens here.
 */
public final class Log implements AutoCloseable {

    private final PartitionId partitionId;
    private final Path dir;
    private final int segmentBytes;
    private final long segmentMillis;
    private final int indexIntervalBytes;
    private final List<LogSegment> sealedSegments = new ArrayList<>();
    private LogSegment active;
    private long activeOpenedAtMs;

    public Log(PartitionId partitionId, Path dir, int segmentBytes, long segmentMillis,
               int indexIntervalBytes) throws IOException {
        this.partitionId = partitionId;
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.segmentMillis = segmentMillis;
        this.indexIntervalBytes = indexIntervalBytes;
        Files.createDirectories(dir);
        rehydrateExistingSegments();
        if (active == null) {
            this.active = new LogSegment(segmentPath(Offset.ZERO), Offset.ZERO, indexIntervalBytes);
            this.activeOpenedAtMs = System.currentTimeMillis();
        }
    }

    private Path segmentPath(Offset baseOffset) {
        return dir.resolve(String.format("%020d.log", baseOffset.value()));
    }

    private void rehydrateExistingSegments() throws IOException {
        if (!Files.exists(dir)) return;
        List<Path> files;
        try (Stream<Path> stream = Files.list(dir)) {
            files = stream
                .filter(p -> p.getFileName().toString().endsWith(".log"))
                .sorted(Comparator.comparing(Path::getFileName))
                .toList();
        }
        if (files.isEmpty()) return;
        for (int i = 0; i < files.size(); i++) {
            Path f = files.get(i);
            String name = f.getFileName().toString();
            long base = Long.parseLong(name.substring(0, name.length() - 4));
            LogSegment seg = new LogSegment(f, new Offset(base), indexIntervalBytes);
            if (i < files.size() - 1) {
                seg.seal();
                sealedSegments.add(seg);
            } else {
                this.active = seg;
                this.activeOpenedAtMs = System.currentTimeMillis();
            }
        }
    }

    /**
     * Append a batch of records under the given leader epoch. The records'
     * offsets are assigned monotonically; the returned {@link AppendResult}
     * carries the assigned base offset.
     */
    public synchronized AppendResult append(List<Record> records, LeaderEpoch epoch) throws IOException {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("cannot append empty record list");
        }
        rollIfNeeded();
        Offset base = active.nextOffset();
        long firstTs = records.get(0).timestampMs();
        RecordBatch batch = new RecordBatch(base, epoch, firstTs, records);
        long position = active.append(batch);
        return new AppendResult(base, batch.lastOffset(), position);
    }

    /**
     * Append a pre-stamped batch (used by the replica layer when replicating
     * batches written by a leader, preserving their offsets and epoch).
     */
    public synchronized AppendResult appendBatch(RecordBatch batch) throws IOException {
        rollIfNeeded();
        if (!batch.baseOffset().equals(active.nextOffset())) {
            throw new IllegalArgumentException(
                "batch baseOffset " + batch.baseOffset()
                    + " does not match log end offset " + active.nextOffset());
        }
        long position = active.append(batch);
        return new AppendResult(batch.baseOffset(), batch.lastOffset(), position);
    }

    private void rollIfNeeded() throws IOException {
        boolean rollOnSize = active.sizeBytes() >= segmentBytes;
        boolean rollOnAge  = (System.currentTimeMillis() - activeOpenedAtMs) >= segmentMillis;
        if ((rollOnSize || rollOnAge) && active.sizeBytes() > 0) {
            active.seal();
            sealedSegments.add(active);
            Offset newBase = active.nextOffset();
            this.active = new LogSegment(segmentPath(newBase), newBase, indexIntervalBytes);
            this.activeOpenedAtMs = System.currentTimeMillis();
        }
    }

    /** Read the batch covering the given offset, scanning sealed segments first. */
    public synchronized Optional<RecordBatch> read(Offset target) throws IOException {
        for (LogSegment seg : sealedSegments) {
            if (target.compareTo(seg.baseOffset()) >= 0 && target.compareTo(seg.nextOffset()) < 0) {
                return seg.read(target);
            }
        }
        return active.read(target);
    }

    /**
     * Read up to {@code maxBytes} of batches starting at {@code from}, walking
     * across segment boundaries as needed.
     */
    public synchronized List<RecordBatch> readBatches(Offset from, int maxBytes) throws IOException {
        List<RecordBatch> out = new ArrayList<>();
        int remaining = maxBytes;
        for (LogSegment seg : sealedSegments) {
            if (from.compareTo(seg.nextOffset()) >= 0) continue;
            for (RecordBatch b : seg.readBatches(from, remaining)) {
                out.add(b);
                remaining -= (4 + 28 + (b.toBytes().length - 32));
                if (remaining <= 0) return out;
            }
        }
        if (from.compareTo(active.baseOffset()) < 0 && !out.isEmpty()) {
            from = out.get(out.size() - 1).lastOffset().plus(1);
        }
        if (remaining > 0) {
            for (RecordBatch b : active.readBatches(from, remaining)) {
                out.add(b);
            }
        }
        return out;
    }

    public Offset logEndOffset() {
        return active.nextOffset();
    }

    public PartitionId partitionId() {
        return partitionId;
    }

    public Path dir() {
        return dir;
    }

    /** Number of sealed segments (does not include the active one). */
    public int sealedSegmentCount() {
        return sealedSegments.size();
    }

    /**
     * Drop sealed segments whose all-records' baseOffset is below
     * {@code retainFromOffset}. The active segment is never deleted.
     */
    public synchronized int deleteSegmentsBelow(Offset retainFromOffset) throws IOException {
        int dropped = 0;
        var it = sealedSegments.iterator();
        while (it.hasNext()) {
            LogSegment seg = it.next();
            if (seg.nextOffset().compareTo(retainFromOffset) <= 0) {
                seg.close();
                Files.deleteIfExists(seg.file());
                it.remove();
                dropped++;
            }
        }
        return dropped;
    }

    @Override
    public synchronized void close() throws IOException {
        for (LogSegment seg : sealedSegments) {
            seg.close();
        }
        active.close();
    }

    public record AppendResult(Offset baseOffset, Offset lastOffset, long physicalPosition) {}
}
