package com.hkg.broker.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommonTypesTest {

    @Test
    void topic_validates_legal_name() {
        new Topic("orders");
        new Topic("user-events_v2.1");
        assertThatThrownBy(() -> new Topic("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Topic("bad name")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Topic("a".repeat(250))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partition_id_renders_kafka_style() {
        PartitionId id = new PartitionId(new Topic("orders"), 7);
        assertThat(id.toString()).isEqualTo("orders-7");
    }

    @Test
    void partition_id_rejects_negative() {
        assertThatThrownBy(() -> new PartitionId(new Topic("t"), -1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void offset_supports_arithmetic_and_ordering() {
        Offset a = new Offset(10);
        Offset b = a.plus(5);
        assertThat(b.value()).isEqualTo(15L);
        assertThat(a.compareTo(b)).isNegative();
        assertThat(Offset.ZERO.value()).isZero();
        assertThatThrownBy(() -> new Offset(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leader_epoch_increments() {
        LeaderEpoch e1 = LeaderEpoch.INITIAL;
        LeaderEpoch e2 = e1.next();
        assertThat(e2.value()).isEqualTo(1);
        assertThat(e1.compareTo(e2)).isNegative();
        assertThatThrownBy(() -> new LeaderEpoch(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void record_defensive_copies_key_and_value() {
        byte[] key = "k".getBytes();
        byte[] val = "v".getBytes();
        Record r = new Record(key, val, 100L);
        key[0] = '!';
        val[0] = '!';
        assertThat(r.key()).contains("k".getBytes());
        assertThat(r.value()).containsExactly("v".getBytes());
    }

    @Test
    void record_allows_null_key() {
        Record r = new Record(null, "payload".getBytes(), 1L);
        assertThat(r.key()).isEmpty();
    }
}
