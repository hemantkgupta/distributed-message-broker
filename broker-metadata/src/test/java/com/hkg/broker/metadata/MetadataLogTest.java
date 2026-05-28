package com.hkg.broker.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataLogTest {

    @Test
    void appends_assign_monotonic_offsets() {
        MetadataLog log = new MetadataLog();
        assertThat(log.append(new MetadataRecord.RegisterBroker(1, "h1", 9092, "r1"))).isZero();
        assertThat(log.append(new MetadataRecord.RegisterBroker(2, "h2", 9092, "r1"))).isOne();
        assertThat(log.highWatermark()).isEqualTo(2L);
    }

    @Test
    void tail_from_zero_returns_full_log() {
        MetadataLog log = new MetadataLog();
        log.append(new MetadataRecord.RegisterBroker(1, "h1", 9092, "r1"));
        log.append(new MetadataRecord.RegisterBroker(2, "h2", 9092, "r1"));
        log.append(new MetadataRecord.CreateTopic("t", 4, 2));
        List<MetadataLog.Entry> tail = log.tailFrom(0);
        assertThat(tail).hasSize(3);
        assertThat(tail.get(0).offset()).isZero();
    }

    @Test
    void tail_from_partial_returns_only_newer_entries() {
        MetadataLog log = new MetadataLog();
        log.append(new MetadataRecord.RegisterBroker(1, "h1", 9092, "r1"));
        log.append(new MetadataRecord.RegisterBroker(2, "h2", 9092, "r1"));
        log.append(new MetadataRecord.CreateTopic("t", 4, 2));
        List<MetadataLog.Entry> tail = log.tailFrom(2);
        assertThat(tail).hasSize(1);
        assertThat(tail.get(0).record()).isInstanceOf(MetadataRecord.CreateTopic.class);
    }

    @Test
    void tail_past_end_returns_empty() {
        MetadataLog log = new MetadataLog();
        log.append(new MetadataRecord.RegisterBroker(1, "h1", 9092, "r1"));
        assertThat(log.tailFrom(99)).isEmpty();
    }
}
