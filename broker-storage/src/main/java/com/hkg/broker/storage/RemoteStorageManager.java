package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.RecordBatch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Storage backend for sealed segments that have moved out of local broker disk.
 */
public interface RemoteStorageManager {

    RemoteSegmentMetadata copySegment(
        PartitionId partition,
        Path localSegmentFile,
        Offset baseOffset,
        Offset nextOffset,
        long sizeBytes
    ) throws IOException;

    List<RecordBatch> read(RemoteSegmentMetadata metadata, Offset from, int maxBytes) throws IOException;
}
