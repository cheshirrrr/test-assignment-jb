package com.jetbrains.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Optional wrapper class that allows to add gzipping to any {@link FileSystem}.
 * Zips files before writing and unzips them after reading
 */
public class GzippedFileSystem implements FileSystem {
    public static final int DEFAULT_BUFFER_SIZE = 1024;

    private final FileSystem delegate;

    public GzippedFileSystem(FileSystem delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean exists(String path) {
        return delegate.exists(path);
    }

    @Override
    public Set<String> listFiles(String path) {
        return delegate.listFiles(path);
    }

    @Override
    public byte[] read(String path) throws IOException {
        byte[] contents = delegate.read(path);
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
    public void write(String path, byte[] contents, boolean overwrite) throws IOException {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(contents); ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); GZIPOutputStream gzippedOut = new GZIPOutputStream(byteOut)) {
            byte[] buffer = new byte[Math.min(contents.length, DEFAULT_BUFFER_SIZE)];
            int len;
            while ((len = byteIn.read(buffer)) > 0) {
                gzippedOut.write(buffer, 0, len);
            }
            gzippedOut.finish();
            byte[] gzippedContents = byteOut.toByteArray();
            delegate.write(path, gzippedContents, overwrite);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        delegate.delete(path);
    }

    @Override
    public List<String> findFile(String fileName) {
        return delegate.findFile(fileName);
    }
}
