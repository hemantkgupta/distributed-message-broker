package com.hkg.broker.storage;

import com.hkg.broker.common.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SparseIndexTest {

    @Test
    void records_first_entry_immediately() {
        SparseIndex idx = new SparseIndex(4096);
        idx.maybeAppend(new Offset(0), 0L, 100);
        assertThat(idx.size()).isEqualTo(1);
        assertThat(idx.lookupFloor(new Offset(0))).isZero();
    }

    @Test
    void skips_entries_within_interval_then_emits() {
        SparseIndex idx = new SparseIndex(1000);
        idx.maybeAppend(new Offset(0), 0L, 400);     // entry @ 0
        idx.maybeAppend(new Offset(10), 400L, 400);  // skipped (within 1000)
        idx.maybeAppend(new Offset(20), 800L, 400);  // skipped (within 1000)
        idx.maybeAppend(new Offset(30), 1200L, 400); // emitted (> 1000 bytes since last)
        assertThat(idx.size()).isEqualTo(2);
    }

    @Test
    void lookup_floor_returns_largest_offset_at_or_below_target() {
        SparseIndex idx = new SparseIndex(1);
        idx.maybeAppend(new Offset(0), 0L, 10);
        idx.maybeAppend(new Offset(100), 100L, 10);
        idx.maybeAppend(new Offset(200), 200L, 10);
        idx.maybeAppend(new Offset(300), 300L, 10);
        assertThat(idx.lookupFloor(new Offset(150))).isEqualTo(100L);
        assertThat(idx.lookupFloor(new Offset(300))).isEqualTo(300L);
        assertThat(idx.lookupFloor(new Offset(50))).isEqualTo(0L);
    }

    @Test
    void empty_index_returns_negative() {
        SparseIndex idx = new SparseIndex(4096);
        assertThat(idx.lookupFloor(new Offset(0))).isEqualTo(-1L);
    }

    @Test
    void positive_interval_required() {
        assertThatThrownBy(() -> new SparseIndex(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SparseIndex(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
