package com.hkg.broker.common;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * A single record payload. Both key and value are byte arrays; defensive
 * copies happen on construction and accessor methods.
 *
 * <p>The broker is intentionally agnostic to record content: only its size and
 * the optional key (for log compaction and partition hashing) are inspected by
 * the storage layer.
 */
public final class Record {

    private final byte[] key;
    private final byte[] value;
    private final long timestampMs;

    public Record(byte[] key, byte[] value, long timestampMs) {
        Objects.requireNonNull(value, "value");
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestamp < 0: " + timestampMs);
        }
        this.key = key == null ? null : key.clone();
        this.value = value.clone();
        this.timestampMs = timestampMs;
    }

    public Optional<byte[]> key() {
        return key == null ? Optional.empty() : Optional.of(key.clone());
    }

    public byte[] value() {
        return value.clone();
    }

    public long timestampMs() {
        return timestampMs;
    }

    /** Wire-size estimate including length prefixes. */
    public int sizeBytes() {
        int keySize = key == null ? 0 : key.length;
        return 4 + 4 + 8 + keySize + value.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record other)) return false;
        return timestampMs == other.timestampMs
            && Arrays.equals(key, other.key)
            && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value), timestampMs);
    }

    @Override
    public String toString() {
        return "Record{ts=" + timestampMs
            + ", key=" + (key == null ? "null" : key.length + "B")
            + ", value=" + value.length + "B}";
    }
}
