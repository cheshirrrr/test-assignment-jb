package com.jetbrains.test;

import com.jetbrains.test.channels.LimitedReadChannel;
import com.jetbrains.test.channels.ListenableWriteChannel;
import org.apache.commons.io.FileExistsException;

import java.io.*;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link FileSystem} interface
 * <p>
 * Stores all files sequentially in a single binary file
 * <p>
 * New files are always appended to the end of the file
 * <p>
 * File deletion is handled by setting "deleted" flag on a specific file
 * <p>
 * File updates are handled by setting "deleted" flag on old copy and appending updated file at the end of the filesystem file
 * <p>
 * Optional cleanup strategies are introduced to compact the file periodically and get rid of deleted files.
 * By default never cleans up the file, which avoids unnecessary confusion
 * but could lead to performance problems if a lot of big files are written
 * <p>
 * Potential improvements:
 * <p>
 * 1. Could be modified to reuse empty spaces within the file for writing new files if they fit.
 * That would allow for more optimal usage of space, but will lead to significantly more complex code
 * (clearing old data, finding a place where file will fit etc.)
 * <p>
 * 2. File streaming could be implemented using {@link RandomAccessFile#getChannel()} which returns {@link java.nio.channels.FileChannel}
 */
public class SingleFileSystem implements FileSystem {

    //  Map for quick access to metadata for all files. allows to quickly find a file in the filesystem file
    private final Map<String, FileMetadata> files = new HashMap<>();

    //  Configuration property that allows to control how many deleted files are allowed in the filesystem file
    private final float fillRate;

    //  Strategy to cleanup deleted files
    private final CleanupStrategy cleanupStrategy;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();
    private final String systemFileName;
    //  Fields that help with the cleanup decision
    private int deletedFileCount = 0;
    private long deletedFileSize = 0;

    private SingleFileSystem(String fileSystemFile, float fillRate, CleanupStrategy cleanupStrategy) {
        systemFileName = fileSystemFile;
        this.fillRate = fillRate;
        this.cleanupStrategy = cleanupStrategy;
    }

    public static FileSystem create(String file) throws IOException {
        return create(file, CleanupStrategy.NEVER, 0f);
    }

    public static FileSystem create(String file, CleanupStrategy cleanupStrategy, float fillRate) throws IOException {
        SingleFileSystem system = new SingleFileSystem(file, fillRate, cleanupStrategy);
        Path path = Paths.get(file);
        boolean fileExists = Files.exists(path);
        if (!fileExists) {
            Files.createFile(path);
        }
        system.initialize();
        return system;
    }

    @Override
    public boolean exists(String path) {
        readLock.lock();
        try {
            return files.keySet().stream().anyMatch(f -> f.startsWith(path));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<String> listFiles(String path) {
        readLock.lock();
        try {
            return files.keySet().stream().filter(s -> s.startsWith(path)).collect(Collectors.toSet());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] read(String path) throws IOException {
        readLock.lock();
        try {
            return readFile(this.systemFileName, path);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void write(String path, byte[] contents, boolean overwrite) throws IOException {
        writeLock.lock();
        try {
            boolean fileExists = files.containsKey(path);
            if (fileExists && !overwrite) {
                throw new FileExistsException(new File(path));
            } else if (fileExists) {
                deleteFile(systemFileName, path);
            }
            writeFile(systemFileName, path, contents);
            cleanUpIfNecessary();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ReadableByteChannel getReadChannel(String path) throws IOException {
        readLock.lock();
        try {
            RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "r");
            if (!files.containsKey(path)) {
                throw new FileNotFoundException(path);
            }
            FileMetadata metadata = files.get(path);
            int fileSize = metadata.getFileSize();
            long offset = metadata.getOffset();
            return new LimitedReadChannel(systemFile.getChannel().position(offset), fileSize);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public WritableByteChannel getWriteChannel(String path, boolean overwrite) throws IOException {
        ListenableWriteChannel channel;
        RandomAccessFile systemFile;
        long sizeOffset;
        long offset;

        writeLock.lock();
        try {
            boolean fileExists = files.containsKey(path);
            if (fileExists && !overwrite) {
                throw new FileExistsException(new File(path));
            } else if (fileExists) {
                deleteFile(systemFileName, path);
            }
            cleanUpIfNecessary();
            systemFile = new RandomAccessFile(systemFileName, "rw");
            systemFile.seek(systemFile.length());
            systemFile.writeUTF(path);
            sizeOffset = systemFile.getFilePointer();
            systemFile.writeInt(0); //temp size to fill in later
            systemFile.writeBoolean(false);
            offset = systemFile.getFilePointer();
            channel = new ListenableWriteChannel(systemFile.getChannel());
        channel.setCloseListener(bytesWritten -> {
            try {
                systemFile.seek(sizeOffset);
                systemFile.writeInt(bytesWritten);
                files.put(path, new FileMetadata(bytesWritten, offset));
            } finally {
                writeLock.unlock();
            }
        });
        return channel;

        } catch (IOException e){
//          catch is used instead of finally because if there is no error
//          then lock should only be released when we finish writing the file
//          and not when we simply return the channel
            writeLock.unlock();
            throw e;
        }
    }

    @Override
    public void delete(String path) throws IOException {
        writeLock.lock();
        try {
            if (!files.containsKey(path)) {
                throw new FileNotFoundException(path);
            }
            deleteFile(systemFileName, path);
            cleanUpIfNecessary();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<String> findFile(String fileName) {
        readLock.lock();
        try {
            return files.keySet().stream().filter(f -> f.endsWith(fileName)).collect(Collectors.toList());
        } finally {
            readLock.unlock();
        }
    }

    protected void cleanUp() throws IOException {
        String tempFileName = systemFileName + UUID.randomUUID().toString();
        Path tempFilePath = Paths.get(tempFileName);
        Files.createFile(tempFilePath);

        Set<String> fileNames = listFiles("");
        for (String fileName : fileNames) {
            byte[] contents = readFile(systemFileName, fileName);
            writeFile(tempFileName, fileName, contents);
        }

        Path systemFilePath = Paths.get(systemFileName);
        Files.delete(systemFilePath);
        Files.move(tempFilePath, systemFilePath);

        deletedFileSize = 0;
        deletedFileCount = 0;
    }


    private void initialize() throws IOException {
        readLock.lock();
        readAllFiles();
        readLock.unlock();
    }

    private void readAllFiles() throws IOException {
        try (RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
            long offset = 0;
            long fileLength = systemFile.length();
            while (offset < fileLength) {
                systemFile.seek(offset);
                String fileName = systemFile.readUTF();
                int size = systemFile.readInt();
                boolean deleted = systemFile.readBoolean();
                long fileOffset = systemFile.getFilePointer();
                if (deleted) {
                    deletedFileCount++;
                    deletedFileSize += size;
                } else {
                    files.put(fileName, new FileMetadata(size, fileOffset));
                }
                offset = fileOffset + size;
            }
        }
    }

    private byte[] readFile(String systemFileName, String path) throws IOException {
        try (RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "r")) {
            if (!files.containsKey(path)) {
                throw new FileNotFoundException(path);
            }
            FileMetadata metadata = files.get(path);
            int fileSize = metadata.getFileSize();
            long offset = metadata.getOffset();
            byte[] contents = new byte[fileSize];
            systemFile.seek(offset);
            systemFile.read(contents);
            systemFile.close();
            return contents;
        }
    }

    private void writeFile(String systemFileName, String fileName, byte[] contents) throws IOException {
        try (RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
            systemFile.seek(systemFile.length());
            systemFile.writeUTF(fileName);
            int fileSize = contents.length;
            systemFile.writeInt(fileSize);
            systemFile.writeBoolean(false);
            long offset = systemFile.getFilePointer();
            systemFile.write(contents);
            files.put(fileName, new FileMetadata(fileSize, offset));
        }
    }

    private void deleteFile(String systemFileName, String path) throws IOException {
        try (RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
            FileMetadata meta = files.remove(path);
            long offset = meta.getOffset();
            systemFile.seek(offset - 1);
            systemFile.writeBoolean(true);
            deletedFileSize += meta.getFileSize();
            deletedFileCount += 1;
        }
    }

    private void cleanUpIfNecessary() throws IOException {
        if (shouldCleanup()) {
            cleanUp();
        }
    }

    private boolean shouldCleanup() {
        switch (cleanupStrategy) {
            case ALWAYS:
                return true;
            case CHECK_COUNT:
                int existingFileCount = files.size();

//              compares number of deleted files with number of deleted and existing files together using fillRate
                return deletedFileCount >= Math.ceil((existingFileCount + deletedFileCount) * fillRate);
            case CHECK_SIZE:
                int existingFilesSize = files.values().stream().mapToInt(FileMetadata::getFileSize).sum();

//              compares total size of deleted files with total size of deleted and existing files together using fillRate
                return deletedFileSize >= Math.ceil((existingFilesSize + deletedFileCount) * fillRate);
            default:
                return false;
        }
    }

    public enum CleanupStrategy {
        NEVER,
        ALWAYS,
        CHECK_COUNT,
        CHECK_SIZE
    }
}
