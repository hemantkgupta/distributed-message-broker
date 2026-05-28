package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * One immutable-once-sealed segment of a partition log.
 *
 * <p>Active segments accept appends; sealed segments are read-only. The
 * on-disk file holds the verbatim wire format of consecutive {@link RecordBatch}
 * frames, each prefixed with its 4-byte length so it can be skipped or
 * positionally addressed during a scan.
 *
 * <p>Each segment owns a {@link SparseIndex} that maps logical offsets to
 * physical byte positions; the index is rebuilt from disk on construction
 * when the segment file already exists.
 */
public final class LogSegment implements AutoCloseable {

    private final Path file;
    private final Offset baseOffset;
    private final SparseIndex index;
    private final FileChannel channel;
    private long size;
    private Offset nextOffset;
    private boolean sealed;

    public LogSegment(Path file, Offset baseOffset, int indexIntervalBytes) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.index = new SparseIndex(indexIntervalBytes);
        Files.createDirectories(file.getParent());
        this.channel = FileChannel.open(file,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        this.size = channel.size();
        if (size > 0) {
            this.nextOffset = rehydrateIndexAndComputeNextOffset();
        } else {
            this.nextOffset = baseOffset;
        }
    }

    private Offset rehydrateIndexAndComputeNextOffset() throws IOException {
        long pos = 0;
        Offset next = baseOffset;
        while (pos < size) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, pos);
            lenBuf.flip();
            int batchLength = lenBuf.getInt();
            int totalSize = 4 + batchLength;
            ByteBuffer batchBuf = ByteBuffer.allocate(totalSize);
            channel.read(batchBuf, pos);
            RecordBatch batch = RecordBatch.fromBytes(batchBuf.array());
            index.maybeAppend(batch.baseOffset(), pos, totalSize);
            next = batch.lastOffset().plus(1);
            pos += totalSize;
        }
        return next;
    }

    /**
     * Append a batch to the active segment, returning its physical position.
     *
     * <p>The batch's base offset must equal the segment's {@code nextOffset};
     * the storage layer guarantees this by stamping offsets as part of the
     * append pipeline.
     */
    public synchronized long append(RecordBatch batch) throws IOException {
        if (sealed) {
            throw new IllegalStateException("segment is sealed: " + file);
        }
        if (!batch.baseOffset().equals(nextOffset)) {
            throw new IllegalArgumentException(
                "batch baseOffset " + batch.baseOffset()
                    + " != segment nextOffset " + nextOffset);
        }
        byte[] bytes = batch.toBytes();
        long position = size;
        channel.write(ByteBuffer.wrap(bytes), position);
        index.maybeAppend(batch.baseOffset(), position, bytes.length);
        size += bytes.length;
        nextOffset = batch.lastOffset().plus(1);
        return position;
    }

    /**
     * Read the batch that contains the given offset. Uses the sparse index to
     * locate the nearest indexed position, then scans forward.
     */
    public synchronized Optional<RecordBatch> read(Offset target) throws IOException {
        if (target.compareTo(baseOffset) < 0 || target.compareTo(nextOffset) >= 0) {
            return Optional.empty();
        }
        long floorPos = index.lookupFloor(target);
        if (floorPos < 0) floorPos = 0L;
        long cursor = floorPos;
        while (cursor < size) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, cursor);
            lenBuf.flip();
            int batchLength = lenBuf.getInt();
            int totalSize = 4 + batchLength;
            ByteBuffer batchBuf = ByteBuffer.allocate(totalSize);
            channel.read(batchBuf, cursor);
            RecordBatch batch = RecordBatch.fromBytes(batchBuf.array());
            if (target.compareTo(batch.baseOffset()) >= 0
                && target.compareTo(batch.lastOffset()) <= 0) {
                return Optional.of(batch);
            }
            cursor += totalSize;
        }
        return Optional.empty();
    }

    /**
     * Read all batches starting at or after {@code from}, up to
     * {@code maxBytes} of accumulated wire-size.
     */
    public synchronized List<RecordBatch> readBatches(Offset from, int maxBytes) throws IOException {
        List<RecordBatch> out = new ArrayList<>();
        if (from.compareTo(nextOffset) >= 0) return out;
        long floorPos = index.lookupFloor(from);
        if (floorPos < 0) floorPos = 0L;
        long cursor = floorPos;
        int accumulated = 0;
        while (cursor < size && accumulated < maxBytes) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, cursor);
            lenBuf.flip();
            int batchLength = lenBuf.getInt();
            int totalSize = 4 + batchLength;
            ByteBuffer batchBuf = ByteBuffer.allocate(totalSize);
            channel.read(batchBuf, cursor);
            RecordBatch batch = RecordBatch.fromBytes(batchBuf.array());
            if (batch.lastOffset().compareTo(from) >= 0) {
                out.add(batch);
                accumulated += totalSize;
            }
            cursor += totalSize;
        }
        return out;
    }

    public synchronized void seal() {
        this.sealed = true;
    }

    public boolean isSealed() {
        return sealed;
    }

    public long sizeBytes() {
        return size;
    }

    public Offset baseOffset() {
        return baseOffset;
    }

    public Offset nextOffset() {
        return nextOffset;
    }

    public Path file() {
        return file;
    }

    public SparseIndex index() {
        return index;
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
