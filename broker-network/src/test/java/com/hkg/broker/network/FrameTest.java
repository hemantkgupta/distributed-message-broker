package com.hkg.broker.network;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FrameTest {

    @Test
    void encode_decode_round_trips() {
        byte[] payload = "hello-broker".getBytes();
        Frame original = new Frame(42, RequestType.PRODUCE, payload);
        Frame decoded = Frame.fromBytes(original.toBytes());

        assertThat(decoded.correlationId()).isEqualTo(42);
        assertThat(decoded.type()).isEqualTo(RequestType.PRODUCE);
        assertThat(decoded.payload()).containsExactly(payload);
    }

    @Test
    void empty_payload_round_trips() {
        Frame f = new Frame(1, RequestType.METADATA, new byte[0]);
        Frame decoded = Frame.fromBytes(f.toBytes());
        assertThat(decoded.payload()).isEmpty();
    }

    @Test
    void length_prefix_matches_remaining_bytes() {
        Frame f = new Frame(7, RequestType.FETCH, "abc".getBytes());
        byte[] bytes = f.toBytes();
        // First 4 bytes = length of remainder
        int declaredLen = (bytes[0] & 0xff) << 24 | (bytes[1] & 0xff) << 16 | (bytes[2] & 0xff) << 8 | (bytes[3] & 0xff);
        assertThat(declaredLen).isEqualTo(bytes.length - 4);
    }
}
