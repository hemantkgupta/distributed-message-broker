package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Metadata for one sealed segment copied to remote storage.
 *
 * <p>The {@code COPY_SEGMENT_FINISHED} state is the important invariant:
 * local deletion is only safe after the remote copy reaches that state.
 */
public record RemoteSegmentMetadata(
    PartitionId partition,
    Offset baseOffset,
    Offset nextOffset,
    Path remotePath,
    long sizeBytes,
    CopyState state
) {
    public RemoteSegmentMetadata {
        Objects.requireNonNull(partition);
        Objects.requireNonNull(baseOffset);
        Objects.requireNonNull(nextOffset);
        Objects.requireNonNull(remotePath);
        Objects.requireNonNull(state);
        if (nextOffset.compareTo(baseOffset) <= 0) {
            throw new IllegalArgumentException("nextOffset must exceed baseOffset");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative");
        }
    }

    public boolean copyFinished() {
        return state == CopyState.COPY_SEGMENT_FINISHED;
    }

    public enum CopyState {
        COPY_SEGMENT_STARTED,
        COPY_SEGMENT_FINISHED
    }
}
