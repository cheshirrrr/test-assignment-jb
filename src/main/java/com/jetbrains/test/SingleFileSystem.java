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
    private RandomAccessFile file;
    private int deletedFileCount = 0;
    private long deletedFileSize = 0;
    private String systemFileName;

    private final ReentrantLock lock = new ReentrantLock();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    protected SingleFileSystem(float fillRate, CleanupStrategy cleanupStrategy) {
        this.fillRate = fillRate;
        this.cleanupStrategy = cleanupStrategy;
    }

    public static FileSystem create(String file) throws IOException {
        return create(file, CleanupStrategy.NEVER, 0f);
    }

    public static FileSystem create(String file, CleanupStrategy cleanupStrategy, float fillRate) throws IOException {
        SingleFileSystem system = new SingleFileSystem(fillRate, cleanupStrategy);
        Path path = Paths.get(file);
        boolean fileExists = Files.exists(path);
        if (!fileExists) {
            Files.createFile(path);
        }
        system.initialize(file);
        return system;
    }

    private void initialize(String fileSystemFile) throws IOException {
        systemFileName = fileSystemFile;
        file = new RandomAccessFile(systemFileName, "rw");
        lock.lock();
        readAllFiles();
        lock.unlock();
    }

    private void readAllFiles() throws IOException {
        long offset = 0;
        long fileLength = file.length();
        while (offset < fileLength) {
            file.seek(offset);
            String fileName = file.readUTF();
            int size = file.read();
            boolean deleted = file.readBoolean();
            long fileOffset = file.getFilePointer();
            if (deleted) {
                deletedFileCount++;
                deletedFileSize += size;
            } else {
                files.put(fileName, new FileMetadata(size, fileOffset));
            }
            offset = fileOffset + size;
        }
    }

    @Override
    public boolean exists(String fileName) {
        lock.lock();
        boolean contains = files.containsKey(fileName);
        lock.unlock();
        return contains;
    }

    @Override
    public Set<String> listFiles(String path) {
        lock.lock();
        if (path.isEmpty()) {
            return files.keySet();
        }
        Set<String> existingFiles = files.keySet().stream().filter(s -> s.startsWith(path)).collect(Collectors.toSet());
        lock.unlock();
        return existingFiles;
    }

    @Override
    public byte[] readFile(String fileName) throws IOException {
        lock.lock();
        byte[] contents = readFile(file, fileName);
        lock.unlock();
        return contents;
    }

    private byte[] readFile(RandomAccessFile systemFile, String fileName) throws IOException {
        if (!files.containsKey(fileName)) {
            throw new FileNotFoundException();
        }
        FileMetadata metadata = files.get(fileName);
        int fileSize = metadata.getFileSize();
        long offset = metadata.getOffset();
        byte[] contents = new byte[fileSize];
        systemFile.seek(offset);
        systemFile.read(contents);
        return contents;
    }

    @Override
    public void writeFile(String fileName, byte[] contents) throws IOException {
        lock.lock();
        if (files.containsKey(fileName)) {
            deleteFile(file, fileName);
        }
        writeFile(file, fileName, contents);
        cleanUpIfNecessary();
        lock.unlock();
    }

    private void writeFile(RandomAccessFile systemFile, String fileName, byte[] contents) throws IOException {
        systemFile.seek(systemFile.length());
        systemFile.writeUTF(fileName);
        int fileSize = contents.length;
        systemFile.write(fileSize);
        systemFile.writeBoolean(false);
        long offset = systemFile.getFilePointer();
        systemFile.write(contents);
        files.put(fileName, new FileMetadata(fileSize, offset));
    }

    @Override
    public void deleteFile(String fileName) throws IOException {
        lock.lock();
        deleteFile(file, fileName);
        cleanUpIfNecessary();
        lock.unlock();
    }

    private void deleteFile(RandomAccessFile systemFile, String fileName) throws IOException {
        FileMetadata meta = files.get(fileName);
        long offset = meta.getOffset();
        systemFile.seek(offset - 1);
        systemFile.writeBoolean(true);
        files.remove(fileName);
        deletedFileSize += meta.getFileSize();
        deletedFileCount += 1;
    }

    @Override
    public void close() throws IOException {
        file.close();
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

    protected void cleanUp() throws IOException {
        String tempFileName = systemFileName + UUID.randomUUID().toString();
        Path tempFilePath = Paths.get(tempFileName);
        Files.createFile(tempFilePath);
        RandomAccessFile tempFile = new RandomAccessFile(tempFileName, "rw");

        Set<String> fileNames = listFiles("");
        for (String fileName : fileNames) {
            byte[] contents = readFile(file, fileName);
            writeFile(tempFile, fileName, contents);
        }

        file.close();
        tempFile.close();
        Path systemFilePath = Paths.get(systemFileName);
        Files.delete(systemFilePath);
        Files.move(tempFilePath, systemFilePath);

        file = new RandomAccessFile(systemFileName, "rw");
        deletedFileSize = 0;
        deletedFileCount = 0;
    }

    public enum CleanupStrategy{
        NEVER,
        ALWAYS,
        CHECK_COUNT,
        CHECK_SIZE
    }
}
