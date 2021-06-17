package com.jetbrains.test.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Write channel that can report how many bytes in total were written
 */
public class ListenableWriteChannel implements WritableByteChannel {

    private final FileChannel delegate;
    private int totalBytesWritten = 0;
    private ThrowingConsumer<Integer, IOException> closeListener;

    public ListenableWriteChannel(FileChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public int
    write(ByteBuffer src) throws IOException {
        int bytesWritten = delegate.write(src);
        totalBytesWritten += bytesWritten;
        return bytesWritten;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        onClose();
        delegate.close();
    }

    private void onClose() throws IOException {
        if (closeListener != null) {
            closeListener.accept(totalBytesWritten);
        }
    }

    /**
     * Allows to setup a listener that will be executed when all bytes has been written
     *
     * @param closeListener - listener that will be executed later
     */
    public void setCloseListener(ThrowingConsumer<Integer, IOException> closeListener) {
        this.closeListener = closeListener;
    }
}
