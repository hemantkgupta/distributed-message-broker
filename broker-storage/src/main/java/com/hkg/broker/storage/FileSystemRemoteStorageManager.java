package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Filesystem-backed remote storage used by tests and local simulations.
 *
 * <p>It models KIP-405's object-store boundary without adding cloud SDKs:
 * sealed segments are copied to a separate root using a temporary file and
 * atomic rename. The returned metadata is only visible after the rename.
 */
public final class FileSystemRemoteStorageManager implements RemoteStorageManager {

    private final Path root;

    public FileSystemRemoteStorageManager(Path root) {
        this.root = root;
    }

    @Override
    public RemoteSegmentMetadata copySegment(
        PartitionId partition,
        Path localSegmentFile,
        Offset baseOffset,
        Offset nextOffset,
        long sizeBytes
    ) throws IOException {
        Path dest = remotePath(partition, baseOffset);
        Files.createDirectories(dest.getParent());
        Path tmp = dest.resolveSibling(dest.getFileName() + ".partial");
        Files.copy(localSegmentFile, tmp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return new RemoteSegmentMetadata(
            partition,
            baseOffset,
            nextOffset,
            dest,
            sizeBytes,
            RemoteSegmentMetadata.CopyState.COPY_SEGMENT_FINISHED
        );
    }

    @Override
    public List<RecordBatch> read(RemoteSegmentMetadata metadata, Offset from, int maxBytes) throws IOException {
        List<RecordBatch> out = new ArrayList<>();
        if (!metadata.copyFinished() || from.compareTo(metadata.nextOffset()) >= 0) {
            return out;
        }
        byte[] all = Files.readAllBytes(metadata.remotePath());
        int cursor = 0;
        int accumulated = 0;
        while (cursor < all.length && accumulated < maxBytes) {
            ByteBuffer lenBuf = ByteBuffer.wrap(all, cursor, 4);
            int batchLength = lenBuf.getInt();
            int totalSize = 4 + batchLength;
            byte[] batchBytes = new byte[totalSize];
            System.arraycopy(all, cursor, batchBytes, 0, totalSize);
            RecordBatch batch = RecordBatch.fromBytes(batchBytes);
            if (batch.lastOffset().compareTo(from) >= 0) {
                out.add(batch);
                accumulated += totalSize;
            }
            cursor += totalSize;
        }
        return out;
    }

    private Path remotePath(PartitionId partition, Offset baseOffset) {
        return root
            .resolve(partition.topic().name())
            .resolve(Integer.toString(partition.partition()))
            .resolve(String.format("%020d.log", baseOffset.value()));
    }
}
