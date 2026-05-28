package com.hkg.broker.common;

/**
 * A monotonic per-partition record offset.
 *
 * <p>Wrapped as a typed record to prevent accidental mixing with positions,
 * timestamps, leader epochs, or generic long counters.
 */
public record Offset(long value) implements Comparable<Offset> {

    public static final Offset ZERO = new Offset(0L);

    public Offset {
        if (value < 0) {
            throw new IllegalArgumentException("offset < 0: " + value);
        }
    }

    public Offset plus(long delta) {
        return new Offset(value + delta);
    }

    @Override
    public int compareTo(Offset o) {
        return Long.compare(this.value, o.value);
    }
}
