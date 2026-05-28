package com.hkg.broker.coordinator;

import com.hkg.broker.common.LeaderEpoch;
import com.hkg.broker.common.Offset;
import com.hkg.broker.common.PartitionId;
import com.hkg.broker.common.Topic;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetManagerTest {

    private final PartitionId p0 = new PartitionId(new Topic("t"), 0);
    private final PartitionId p1 = new PartitionId(new Topic("t"), 1);

    @Test
    void commit_then_fetch_returns_latest() {
        OffsetManager om = new OffsetManager();
        om.commit("g1", p0, new Offset(10), LeaderEpoch.INITIAL, 100L, null);
        om.commit("g1", p0, new Offset(20), new LeaderEpoch(2), 200L, "rebal");
        assertThat(om.fetch("g1", p0)).hasValueSatisfying(co -> {
            assertThat(co.offset()).isEqualTo(new Offset(20));
            assertThat(co.leaderEpoch()).isEqualTo(new LeaderEpoch(2));
            assertThat(co.commitTimestampMs()).isEqualTo(200L);
            assertThat(co.metadata()).isEqualTo("rebal");
        });
    }

    @Test
    void missing_returns_empty() {
        OffsetManager om = new OffsetManager();
        assertThat(om.fetch("g1", p0)).isEmpty();
    }

    @Test
    void commits_are_isolated_per_group_and_partition() {
        OffsetManager om = new OffsetManager();
        om.commit("g1", p0, new Offset(10), LeaderEpoch.INITIAL, 100L, null);
        om.commit("g2", p0, new Offset(20), LeaderEpoch.INITIAL, 100L, null);
        om.commit("g1", p1, new Offset(30), LeaderEpoch.INITIAL, 100L, null);
        assertThat(om.size()).isEqualTo(3);
        assertThat(om.fetch("g1", p0).get().offset()).isEqualTo(new Offset(10));
        assertThat(om.fetch("g2", p0).get().offset()).isEqualTo(new Offset(20));
        assertThat(om.fetch("g1", p1).get().offset()).isEqualTo(new Offset(30));
    }

    @Test
    void delete_removes_commit() {
        OffsetManager om = new OffsetManager();
        om.commit("g1", p0, new Offset(10), LeaderEpoch.INITIAL, 100L, null);
        om.delete("g1", p0);
        assertThat(om.fetch("g1", p0)).isEmpty();
        assertThat(om.size()).isZero();
    }
}
