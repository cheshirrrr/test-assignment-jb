package com.jetbrains.test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface FileSystem {
    boolean exists(String fileName);

    Set<String> listFiles(String path);

    byte[] read(String path) throws IOException;

    void write(String path, byte[] contents) throws IOException;

    void delete(String path) throws IOException;

    List<String> findFile(String fileName);
}
