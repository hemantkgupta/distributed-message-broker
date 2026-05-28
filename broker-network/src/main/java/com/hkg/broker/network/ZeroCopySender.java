package com.hkg.broker.network;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Pipes a byte range from a regular file directly to a network socket using
 * {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
 *
 * <p>This is the equivalent of Kafka's zero-copy fetch path. On Linux, the
 * kernel implements {@code transferTo} via {@code sendfile(2)} when both
 * sides support it, so the bytes move from the OS page cache directly to
 * the socket's send buffer without ever crossing the JVM user-space
 * boundary. No GC pressure, no double-buffering, no decode pass.
 *
 * <p>On platforms or socket types where {@code sendfile} is unavailable, the
 * JDK falls back to a read+write internally — the API surface is identical.
 */
public final class ZeroCopySender {

    private ZeroCopySender() {}

    /**
     * Transfer {@code length} bytes starting at {@code position} from the
     * file to the target channel. Loops over {@code transferTo} calls until
     * the full range has been written.
     *
     * @return total bytes written
     */
    public static long transfer(Path file, long position, long length, WritableByteChannel target) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            long remaining = length;
            long offset = position;
            long totalWritten = 0;
            while (remaining > 0) {
                long n = fc.transferTo(offset, remaining, target);
                if (n <= 0) break;
                offset += n;
                remaining -= n;
                totalWritten += n;
            }
            return totalWritten;
        }
    }
}
