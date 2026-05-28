package com.hkg.broker.replica;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeaderEpochTrackerTest {

    @Test
    void end_offset_of_latest_epoch_returns_log_end_offset() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(1), new Offset(0));
        assertThat(t.endOffsetOf(new LeaderEpoch(1), new Offset(100)))
            .contains(new Offset(100));
    }

    @Test
    void end_offset_of_older_epoch_returns_start_of_next() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(1), new Offset(0));
        t.recordLeadershipStart(new LeaderEpoch(2), new Offset(50));
        t.recordLeadershipStart(new LeaderEpoch(3), new Offset(120));
        assertThat(t.endOffsetOf(new LeaderEpoch(1), new Offset(200)))
            .contains(new Offset(50));
        assertThat(t.endOffsetOf(new LeaderEpoch(2), new Offset(200)))
            .contains(new Offset(120));
        assertThat(t.endOffsetOf(new LeaderEpoch(3), new Offset(200)))
            .contains(new Offset(200));
    }

    @Test
    void unknown_epoch_returns_empty() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(1), new Offset(0));
        assertThat(t.endOffsetOf(new LeaderEpoch(99), new Offset(0))).isEmpty();
    }

    @Test
    void epochs_must_strictly_increase() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(2), new Offset(0));
        assertThatThrownBy(() -> t.recordLeadershipStart(new LeaderEpoch(2), new Offset(10)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> t.recordLeadershipStart(new LeaderEpoch(1), new Offset(10)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void start_offsets_must_be_monotonic() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(1), new Offset(100));
        assertThatThrownBy(() -> t.recordLeadershipStart(new LeaderEpoch(2), new Offset(50)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void truncate_to_epoch_drops_later_entries() {
        LeaderEpochTracker t = new LeaderEpochTracker();
        t.recordLeadershipStart(new LeaderEpoch(1), new Offset(0));
        t.recordLeadershipStart(new LeaderEpoch(2), new Offset(50));
        t.recordLeadershipStart(new LeaderEpoch(3), new Offset(120));
        t.truncateToEpoch(new LeaderEpoch(2));
        assertThat(t.latestEpoch()).contains(new LeaderEpoch(2));
    }
}
