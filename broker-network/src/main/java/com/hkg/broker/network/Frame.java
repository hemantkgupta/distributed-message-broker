package com.hkg.broker.network;

import java.nio.ByteBuffer;

/**
 * Length-prefixed wire frame for broker requests and responses.
 *
 * <p>Frame layout:
 * <pre>
 *   int32  frameLength       (everything after this field)
 *   int32  correlationId     (echoed in the response)
 *   int8   typeCode          (RequestType ordinal)
 *   bytes  payload[frameLength - 5]
 * </pre>
 *
 * <p>The 4-byte length prefix lets the receiver allocate the right buffer
 * before reading the payload and lets the broker stream a sequence of
 * frames over a single TCP connection without ambiguity.
 */
public record Frame(int correlationId, RequestType type, byte[] payload) {

    public byte[] toBytes() {
        int frameLength = 4 + 1 + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + frameLength);
        buf.putInt(frameLength);
        buf.putInt(correlationId);
        buf.put((byte) type.ordinal());
        buf.put(payload);
        return buf.array();
    }

    public static Frame fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int frameLength = buf.getInt();
        int correlationId = buf.getInt();
        byte typeCode = buf.get();
        RequestType type = RequestType.values()[typeCode];
        byte[] payload = new byte[frameLength - 5];
        buf.get(payload);
        return new Frame(correlationId, type, payload);
    }
}
