package com.jetbrains.test;

import java.io.IOException;
import java.util.Set;

public interface FileSystem {
    boolean exists(String fileName);

    Set<String> listFiles(String path);

    byte[] readFile(String fileName) throws IOException;

    void writeFile(String fileName, byte[] contents) throws IOException;

    void deleteFile(String fileName) throws IOException;

    void close() throws IOException;
}
