package com.hkg.broker.network;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Server-side waiting list for in-flight requests that need to wait on a
 * condition before responding. The canonical examples are:
 *
 * <ul>
 *   <li>{@code DelayedProduce}: an {@code acks=all} produce that must wait
 *       for the HWM to reach the appended offset before the broker can ack
 *       the producer.</li>
 *   <li>{@code DelayedFetch}: a consumer fetch with a min-bytes threshold
 *       that must wait for enough new bytes to accumulate (or for the
 *       max-wait deadline to fire).</li>
 * </ul>
 *
 * <p>This implementation exposes a step function so tests can drive the
 * queue deterministically. The broker housekeeping thread (in a real
 * deployment) would call {@code pollAndComplete} on every replication
 * update + on a timer.
 */
public final class DelayedRequestQueue<R> {

    private final List<Entry<R>> entries = new ArrayList<>();

    /**
     * Register a new request. If {@code condition} already returns true, the
     * request completes immediately and the result is returned directly.
     * Otherwise it is queued and {@link #pollAndComplete(long)} will fire it
     * when the condition becomes true or its deadline passes.
     */
    public synchronized R registerOrCompleteImmediately(
        Supplier<Boolean> condition,
        Supplier<R> onComplete,
        Supplier<R> onTimeout,
        long deadlineMs
    ) {
        if (Boolean.TRUE.equals(condition.get())) {
            return onComplete.get();
        }
        entries.add(new Entry<>(condition, onComplete, onTimeout, deadlineMs));
        return null;
    }

    /**
     * Walk the queue. For every entry whose condition fires, run
     * {@code onComplete}. For every entry past its deadline, run
     * {@code onTimeout}. Returns the results in the order they fired.
     */
    public synchronized List<R> pollAndComplete(long nowMs) {
        List<R> results = new ArrayList<>();
        Iterator<Entry<R>> it = entries.iterator();
        while (it.hasNext()) {
            Entry<R> e = it.next();
            if (Boolean.TRUE.equals(e.condition.get())) {
                results.add(e.onComplete.get());
                it.remove();
            } else if (nowMs >= e.deadlineMs) {
                results.add(e.onTimeout.get());
                it.remove();
            }
        }
        return results;
    }

    public synchronized int size() {
        return entries.size();
    }

    private record Entry<R>(
        Supplier<Boolean> condition,
        Supplier<R> onComplete,
        Supplier<R> onTimeout,
        long deadlineMs
    ) {}
}
