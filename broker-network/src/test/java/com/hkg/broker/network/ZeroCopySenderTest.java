package com.hkg.broker.network;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ZeroCopySenderTest {

    /**
     * A WritableByteChannel that buffers everything written to it. Stands in
     * for a real SocketChannel in unit tests.
     */
    private static final class CollectingChannel implements WritableByteChannel {
        private final ByteBuffer buf = ByteBuffer.allocate(64 * 1024);

        @Override
        public int write(ByteBuffer src) {
            int n = src.remaining();
            buf.put(src);
            return n;
        }

        @Override public boolean isOpen() { return true; }
        @Override public void close() {}

        byte[] toBytes() {
            byte[] copy = new byte[buf.position()];
            System.arraycopy(buf.array(), 0, copy, 0, copy.length);
            return copy;
        }
    }

    @Test
    void transfers_full_byte_range_through_transferTo(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("payload.bin");
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i & 0xff);
        Files.write(file, data);

        CollectingChannel sink = new CollectingChannel();
        long written = ZeroCopySender.transfer(file, 0, 1024, sink);

        assertThat(written).isEqualTo(1024L);
        assertThat(sink.toBytes()).containsExactly(data);
    }

    @Test
    void transfers_arbitrary_subrange(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("payload.bin");
        byte[] data = new byte[2048];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i & 0xff);
        Files.write(file, data);

        CollectingChannel sink = new CollectingChannel();
        long written = ZeroCopySender.transfer(file, 512, 256, sink);

        assertThat(written).isEqualTo(256L);
        byte[] expected = new byte[256];
        System.arraycopy(data, 512, expected, 0, 256);
        assertThat(sink.toBytes()).containsExactly(expected);
    }
}
