package com.jetbrains.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Objects;

public class FileMetadata implements Serializable {
    private final int fileSize;
    private final long offset;

    public FileMetadata(int fileSize, long offset) {
        this.fileSize = fileSize;
        this.offset = offset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata that = (FileMetadata) o;
        return fileSize == that.fileSize &&
                offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSize, offset);
    }
}
