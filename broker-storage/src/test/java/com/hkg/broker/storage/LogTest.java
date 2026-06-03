package com.hkg.broker.storage;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LogTest {

    private PartitionId pid() {
        return new PartitionId(new Topic("orders"), 0);
    }

    private List<Record> records(int n, int payloadBytes) {
        Record[] out = new Record[n];
        byte[] payload = new byte[payloadBytes];
        for (int i = 0; i < n; i++) {
            out[i] = new Record(null, payload, 0L);
        }
        return List.of(out);
    }

    @Test
    void monotonic_offset_assignment(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, 64 * 1024, Long.MAX_VALUE, 128)) {
            Log.AppendResult first = log.append(records(3, 16), LeaderEpoch.INITIAL);
            Log.AppendResult second = log.append(records(2, 16), LeaderEpoch.INITIAL);
            assertThat(first.baseOffset()).isEqualTo(Offset.ZERO);
            assertThat(first.lastOffset()).isEqualTo(new Offset(2));
            assertThat(second.baseOffset()).isEqualTo(new Offset(3));
            assertThat(second.lastOffset()).isEqualTo(new Offset(4));
            assertThat(log.logEndOffset()).isEqualTo(new Offset(5));
        }
    }

    @Test
    void rolls_to_new_segment_on_size(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, /* segmentBytes */ 200, Long.MAX_VALUE, 128)) {
            for (int i = 0; i < 20; i++) {
                log.append(records(1, 64), LeaderEpoch.INITIAL);
            }
            assertThat(log.sealedSegmentCount()).isGreaterThan(0);
        }
    }

    @Test
    void reads_across_segment_boundary(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, /* segmentBytes */ 200, Long.MAX_VALUE, 64)) {
            for (int i = 0; i < 30; i++) {
                log.append(records(1, 64), LeaderEpoch.INITIAL);
            }
            // Read a few offsets that span multiple segments.
            for (long target : new long[]{0, 5, 10, 20, 29}) {
                Optional<RecordBatch> b = log.read(new Offset(target));
                assertThat(b).withFailMessage("expected batch covering offset " + target).isPresent();
                assertThat(b.get().baseOffset().compareTo(new Offset(target))).isLessThanOrEqualTo(0);
                assertThat(b.get().lastOffset().compareTo(new Offset(target))).isGreaterThanOrEqualTo(0);
            }
        }
    }

    @Test
    void retention_drops_old_segments(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, /* segmentBytes */ 200, Long.MAX_VALUE, 64)) {
            for (int i = 0; i < 30; i++) {
                log.append(records(1, 64), LeaderEpoch.INITIAL);
            }
            int sealedBefore = log.sealedSegmentCount();
            assertThat(log.deleteSegmentsBelow(new Offset(20))).isZero();
            assertThat(log.sealedSegmentCount()).isEqualTo(sealedBefore);

            FileSystemRemoteStorageManager remote =
                new FileSystemRemoteStorageManager(tmp.resolve("remote"));
            assertThat(log.offloadSealedSegments(remote)).isGreaterThan(0);
            int dropped = log.deleteSegmentsBelow(new Offset(20));
            assertThat(dropped).isGreaterThan(0);
            assertThat(log.sealedSegmentCount()).isLessThan(sealedBefore);
        }
    }

    @Test
    void offloaded_segment_remains_readable_after_local_delete(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp.resolve("local"), /* segmentBytes */ 200, Long.MAX_VALUE, 64)) {
            for (int i = 0; i < 30; i++) {
                log.append(List.of(new Record(null, ("r" + i).getBytes(), 0L)), LeaderEpoch.INITIAL);
            }
            FileSystemRemoteStorageManager remote =
                new FileSystemRemoteStorageManager(tmp.resolve("remote"));
            log.offloadSealedSegments(remote);
            log.deleteSegmentsBelow(new Offset(20));

            Optional<RecordBatch> batch = log.read(new Offset(0));
            assertThat(batch).isPresent();
            assertThat(new String(batch.get().records().get(0).value())).isEqualTo("r0");
            assertThat(log.remoteSegmentCount()).isGreaterThan(0);
        }
    }

    @Test
    void survives_close_and_reopen(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, 64 * 1024, Long.MAX_VALUE, 128)) {
            for (int i = 0; i < 5; i++) {
                log.append(records(1, 64), LeaderEpoch.INITIAL);
            }
        }
        try (Log reopened = new Log(pid(), tmp, 64 * 1024, Long.MAX_VALUE, 128)) {
            assertThat(reopened.logEndOffset()).isEqualTo(new Offset(5));
        }
    }

    @Test
    void replicated_batch_with_pre_stamped_offset(@TempDir Path tmp) throws IOException {
        try (Log log = new Log(pid(), tmp, 64 * 1024, Long.MAX_VALUE, 128)) {
            log.append(records(2, 16), LeaderEpoch.INITIAL);
            RecordBatch fromLeader = new RecordBatch(
                new Offset(2),
                new LeaderEpoch(4),
                123L,
                records(2, 16)
            );
            Log.AppendResult result = log.appendBatch(fromLeader);
            assertThat(result.baseOffset()).isEqualTo(new Offset(2));
            assertThat(log.logEndOffset()).isEqualTo(new Offset(4));
        }
    }
}
