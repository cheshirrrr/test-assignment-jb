package com.jetbrains.test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SingleFileSystem implements FileSystem {

    private final Map<String, FileMetadata> files = new HashMap<>();
    private final float fillRate;
    private final CleanupStrategy cleanupStrategy;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    private int deletedFileCount = 0;
    private long deletedFileSize = 0;
    private final String systemFileName;

    protected SingleFileSystem(String fileSystemFile, float fillRate, CleanupStrategy cleanupStrategy) {
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
    public boolean exists(String fileName) {
        readLock.lock();
        boolean contains = files.containsKey(fileName);
        readLock.unlock();
        return contains;
    }

    @Override
    public Set<String> listFiles(String path) {
        readLock.lock();
        if (path.isEmpty()) {
            return files.keySet();
        }
        Set<String> existingFiles = files.keySet().stream().filter(s -> s.startsWith(path)).collect(Collectors.toSet());
        readLock.unlock();
        return existingFiles;
    }

    @Override
    public byte[] readFile(String fileName) throws IOException {
        readLock.lock();
        byte[] contents = readFile(this.systemFileName, fileName);
        readLock.unlock();
        return contents;
    }

    @Override
    public void writeFile(String fileName, byte[] contents) throws IOException {
        writeLock.lock();
        if (files.containsKey(fileName)) {
            deleteFile(systemFileName, fileName);
        }
        writeFile(systemFileName, fileName, contents);
        cleanUpIfNecessary();
        writeLock.unlock();
    }

    @Override
    public void deleteFile(String fileName) throws IOException {
        writeLock.lock();
        deleteFile(systemFileName, fileName);
        cleanUpIfNecessary();
        writeLock.unlock();
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
        try(RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
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

    private byte[] readFile(String systemFileName, String fileName) throws IOException {
        try(RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "r")) {
            if (!files.containsKey(fileName)) {
                throw new FileNotFoundException();
            }
            FileMetadata metadata = files.get(fileName);
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
        try(RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
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

    private void deleteFile(String systemFileName, String fileName) throws IOException {
        try(RandomAccessFile systemFile = new RandomAccessFile(systemFileName, "rw")) {
            FileMetadata meta = files.get(fileName);
            long offset = meta.getOffset();
            systemFile.seek(offset - 1);
            systemFile.writeBoolean(true);
            files.remove(fileName);
            deletedFileSize += meta.getFileSize();
            deletedFileCount += 1;
        }
    }

    private void cleanUpIfNecessary() throws IOException {
        if(shouldCleanup()) {
            cleanUp();
        }
    }

    private boolean shouldCleanup() {
        switch (cleanupStrategy) {
            case ALWAYS:
                return true;
            case CHECK_COUNT:
                int existingFileCount = files.size();
                return deletedFileCount >= Math.ceil((existingFileCount + deletedFileCount) * fillRate);
            case CHECK_SIZE:
                int existingFilesSize = files.values().stream().mapToInt(FileMetadata::getFileSize).sum();
                return deletedFileSize >= Math.ceil((existingFilesSize + deletedFileCount) * fillRate);
            default:
                return false;
        }
    }

    public enum CleanupStrategy{
        NEVER,
        ALWAYS,
        CHECK_COUNT,
        CHECK_SIZE
    }
}
