package com.jetbrains.test;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Set;

/**
 * File system interface. Implementations allow rudimentary operations on files
 */
public interface FileSystem {

    /**
     * Checks if specified path exists in the system
     * @param path - path that needs to be checked. Can be path to any folder/file in the system
     * @return - true if any path in the system matches provided path
     */
    boolean exists(String path);

    /**
     * Lists files that belong to specified path
     * @param path - path that needs to be checked. Can be path to any folder/subfolder/file in the system
     * @return - list of absolute paths to files that match specified path
     */
    Set<String> listFiles(String path);

    /**
     * Reads content of a file if it exists in the system
     * @param path - absolute path to the file
     * @return - contents of the file in the form of byte array
     * @throws IOException - if system cannot read specified file
     */
    byte[] read(String path) throws IOException;

    /**
     * Provides a {@link java.nio.channels.Channel} to read data from file
     * @param path - absolute path to the file
     * @return - readable channel that can read contents of the file
     * @throws IOException - if system cannot read specified file
     */
    ReadableByteChannel getReadChannel(String path) throws IOException;

    /**
     * Writes contents of the file to a specified path in the system
     * @param path - absolute path to the file
     * @param contents - contents of the file in the form of byte array
     * @param overwrite - if set to false, system will throw exception if file already exists
     * @throws IOException - if system cannot write file, or if attempting to overwrite file without overwrite flag
     */
    void write(String path, byte[] contents, boolean overwrite) throws IOException;

    /**
     *  Provides a {@link java.nio.channels.Channel} to write file data into filesystem
     * @param path - absolute path to the file
     * @param overwrite - if set to false, system will throw exception if file already exists
     * @throws IOException - if system cannot write file, or if attempting to overwrite file without overwrite flag
     */
    WritableByteChannel getWriteChannel(String path, boolean overwrite) throws IOException;

    /**
     * Deletes existing file
     * @param path  - absolute path to the file
     * @throws IOException - if system cannot delete file or file does not exist in the system
     */
    void delete(String path) throws IOException;

    /**
     * Looks for all the files in the system with specified name
     * Can be useful when looking for the same file in different folders
     * @param fileName - name of the file
     * @return - List of absolute paths to all existing files with specified names
     */
    List<String> findFile(String fileName);
}
