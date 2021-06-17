package com.jetbrains.test.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

/**
 * Read channel with an imposed limit.
 */
public class LimitedReadChannel implements ReadableByteChannel {

    private final FileChannel delegate;
    private final int maxBytes;
    private int readCount = 0;

    public LimitedReadChannel(FileChannel delegate, int maxBytes) {
        this.delegate = delegate;
        this.maxBytes = maxBytes;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (readCount >= maxBytes) {
            return -1;
        }
        int oldLimit = dst.limit();
        int newLimit = Math.min(oldLimit, maxBytes);
        dst.limit(newLimit);
        int read = delegate.read(dst);
        readCount += read;
        dst.limit(oldLimit);
        return read;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
