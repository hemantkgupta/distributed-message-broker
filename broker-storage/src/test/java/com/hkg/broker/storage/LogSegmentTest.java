package com.hkg.broker.storage;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.Record;
import com.hkg.broker.common.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogSegmentTest {

    private RecordBatch batchAt(long base, int count) {
        Record[] records = new Record[count];
        for (int i = 0; i < count; i++) {
            records[i] = new Record(null, ("payload-" + (base + i)).getBytes(), base + i);
        }
        return new RecordBatch(new Offset(base), LeaderEpoch.INITIAL, base, List.of(records));
    }

    @Test
    void append_then_read_round_trips(@TempDir Path tmp) throws IOException {
        try (LogSegment seg = new LogSegment(tmp.resolve("0.log"), Offset.ZERO, 64)) {
            seg.append(batchAt(0, 3));
            seg.append(batchAt(3, 2));
            assertThat(seg.nextOffset()).isEqualTo(new Offset(5));
            assertThat(seg.read(new Offset(0))).get().extracting("baseOffset")
                .isEqualTo(new Offset(0));
            assertThat(seg.read(new Offset(2))).get().extracting("baseOffset")
                .isEqualTo(new Offset(0));
            assertThat(seg.read(new Offset(4))).get().extracting("baseOffset")
                .isEqualTo(new Offset(3));
        }
    }

    @Test
    void sealed_segment_rejects_appends(@TempDir Path tmp) throws IOException {
        try (LogSegment seg = new LogSegment(tmp.resolve("0.log"), Offset.ZERO, 64)) {
            seg.append(batchAt(0, 1));
            seg.seal();
            assertThat(seg.isSealed()).isTrue();
            assertThatThrownBy(() -> seg.append(batchAt(1, 1)))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void wrong_base_offset_rejected(@TempDir Path tmp) throws IOException {
        try (LogSegment seg = new LogSegment(tmp.resolve("0.log"), Offset.ZERO, 64)) {
            seg.append(batchAt(0, 2));
            assertThatThrownBy(() -> seg.append(batchAt(5, 1)))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void index_drives_fast_lookup_under_load(@TempDir Path tmp) throws IOException {
        // Use a small index interval so we exercise multiple index entries.
        try (LogSegment seg = new LogSegment(tmp.resolve("0.log"), Offset.ZERO, 128)) {
            for (int i = 0; i < 100; i++) {
                seg.append(batchAt(i, 1));
            }
            assertThat(seg.index().size()).isGreaterThan(1);
            // Random sample of lookups.
            for (long target : new long[]{0, 1, 17, 42, 73, 99}) {
                Optional<RecordBatch> rb = seg.read(new Offset(target));
                assertThat(rb).isPresent();
                assertThat(rb.get().baseOffset().value()).isEqualTo(target);
            }
        }
    }

    @Test
    void rehydrate_on_reopen_reconstructs_index_and_next_offset(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("100.log");
        try (LogSegment seg = new LogSegment(file, new Offset(100), 128)) {
            seg.append(batchAt(100, 3));
            seg.append(batchAt(103, 2));
        }
        try (LogSegment reopened = new LogSegment(file, new Offset(100), 128)) {
            assertThat(reopened.nextOffset()).isEqualTo(new Offset(105));
            assertThat(reopened.read(new Offset(104))).get().extracting("baseOffset")
                .isEqualTo(new Offset(103));
        }
    }

    @Test
    void read_batches_returns_consecutive_batches(@TempDir Path tmp) throws IOException {
        try (LogSegment seg = new LogSegment(tmp.resolve("0.log"), Offset.ZERO, 64)) {
            seg.append(batchAt(0, 2));
            seg.append(batchAt(2, 2));
            seg.append(batchAt(4, 2));
            List<RecordBatch> batches = seg.readBatches(new Offset(2), 1024);
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0).baseOffset()).isEqualTo(new Offset(2));
            assertThat(batches.get(1).baseOffset()).isEqualTo(new Offset(4));
        }
    }
}
