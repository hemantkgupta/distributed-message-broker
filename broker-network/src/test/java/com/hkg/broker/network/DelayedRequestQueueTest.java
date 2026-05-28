package com.hkg.broker.network;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class DelayedRequestQueueTest {

    @Test
    void completes_immediately_when_condition_already_true() {
        DelayedRequestQueue<String> q = new DelayedRequestQueue<>();
        String result = q.registerOrCompleteImmediately(
            () -> true,
            () -> "done",
            () -> "timeout",
            Long.MAX_VALUE
        );
        assertThat(result).isEqualTo("done");
        assertThat(q.size()).isZero();
    }

    @Test
    void queues_when_condition_false_then_fires_on_poll() {
        DelayedRequestQueue<String> q = new DelayedRequestQueue<>();
        AtomicLong flag = new AtomicLong(0);
        String result = q.registerOrCompleteImmediately(
            () -> flag.get() > 0,
            () -> "done",
            () -> "timeout",
            Long.MAX_VALUE
        );
        assertThat(result).isNull();
        assertThat(q.size()).isOne();

        // Condition still false; poll should not fire.
        List<String> firstPoll = q.pollAndComplete(0);
        assertThat(firstPoll).isEmpty();

        // Flip the condition; next poll fires the entry.
        flag.set(1);
        List<String> secondPoll = q.pollAndComplete(0);
        assertThat(secondPoll).containsExactly("done");
        assertThat(q.size()).isZero();
    }

    @Test
    void fires_timeout_when_deadline_passes() {
        DelayedRequestQueue<String> q = new DelayedRequestQueue<>();
        q.registerOrCompleteImmediately(
            () -> false,
            () -> "done",
            () -> "timeout",
            /* deadline */ 100L
        );
        assertThat(q.pollAndComplete(99)).isEmpty();
        assertThat(q.pollAndComplete(100)).containsExactly("timeout");
        assertThat(q.size()).isZero();
    }

    @Test
    void multiple_entries_fire_independently() {
        DelayedRequestQueue<String> q = new DelayedRequestQueue<>();
        AtomicLong a = new AtomicLong(0);
        AtomicLong b = new AtomicLong(0);

        q.registerOrCompleteImmediately(() -> a.get() > 0, () -> "A", () -> "Atimeout", Long.MAX_VALUE);
        q.registerOrCompleteImmediately(() -> b.get() > 0, () -> "B", () -> "Btimeout", Long.MAX_VALUE);
        assertThat(q.size()).isEqualTo(2);

        a.set(1);
        assertThat(q.pollAndComplete(0)).containsExactly("A");
        assertThat(q.size()).isOne();

        b.set(1);
        assertThat(q.pollAndComplete(0)).containsExactly("B");
        assertThat(q.size()).isZero();
    }
}
