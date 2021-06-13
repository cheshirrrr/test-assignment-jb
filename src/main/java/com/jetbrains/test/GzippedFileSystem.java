package com.jetbrains.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzippedFileSystem implements FileSystem {
    private FileSystem delegate;

    public GzippedFileSystem(FileSystem delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean exists(String fileName) {
        return delegate.exists(fileName);
    }

    @Override
    public Set<String> listFiles(String path) {
        return delegate.listFiles(path);
    }

    @Override
    public byte[] readFile(String fileName) throws IOException {
        byte[] contents = delegate.readFile(fileName);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (GZIPInputStream gzippedIn = new GZIPInputStream(new ByteArrayInputStream(contents))) {
            byte[] buffer = new byte[Math.min(contents.length, 1024)];
            int len;
            while ((len = gzippedIn.read(buffer)) > 0) {
                byteOut.write(buffer, 0, len);
            }
            gzippedIn.close();
            return byteOut.toByteArray();
        }
    }

    @Override
    public void writeFile(String fileName, byte[] contents) throws IOException {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(contents); ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); GZIPOutputStream gzippedOut = new GZIPOutputStream(byteOut)) {
            byte[] buffer = new byte[Math.min(contents.length, 1024)];
            int len;
            while ((len = byteIn.read(buffer)) > 0) {
                gzippedOut.write(buffer, 0, len);
            }
            gzippedOut.finish();
            byte[] gzippedContents = byteOut.toByteArray();
            delegate.writeFile(fileName, gzippedContents);
        }
    }

    @Override
    public void deleteFile(String fileName) throws IOException {
        delegate.deleteFile(fileName);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
