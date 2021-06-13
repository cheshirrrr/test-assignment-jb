package com.jetbrains.test;

import org.apache.commons.io.FileExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SingleFileSystemTest {

    @TempDir
    public static Path tempDirectory;

    public static final String FILE_SYSTEM_FILENAME = "testfile";

    public static Path SYSTEM_FILE_PATH;

    @BeforeAll
    public static void deleteFile() throws IOException {
        Path path = tempDirectory.resolve(FILE_SYSTEM_FILENAME);
        Files.deleteIfExists(path);

        SYSTEM_FILE_PATH = tempDirectory.resolve(FILE_SYSTEM_FILENAME);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(SYSTEM_FILE_PATH);
    }

    @Test
    @DisplayName("When new file is added it can be read back")
    public void canCreateFile() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing write/read";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.write(testFile, testString.getBytes(), true);

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.read(testFile);
        assertEquals(testString, new String(bytes));
    }

    @Test
    @DisplayName("When new file is updated new version can be read back")
    public void canUpdateFile() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing write/read";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.write(testFile, testString.getBytes(), true);

        String testStringUpdated = testString + 2;
        system.write(testFile, testStringUpdated.getBytes(), true);

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.read(testFile);
        assertEquals(testStringUpdated, new String(bytes));
    }

    @Test
    @DisplayName("Trying to write to existing file without 'overwrite' flag leads to Exception")
    public void cannotOverwriteFile() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing write";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.write(testFile, testString.getBytes(), false);

        assertThrows(FileExistsException.class, () -> system.write(testFile, testString.getBytes(), false));
    }

    @Test
    @DisplayName("When file is deleted reading it throws FileNotFoundException")
    public void cannotFindDeleted() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing deletion";

        String fileNameBase = "testfolder/testsubfolder/testfile";
        for (int i = 0; i < 10; i++) {

            system.write(fileNameBase + i, testString.getBytes(), true);
        }


        String testFile = fileNameBase + "1";
        assertTrue(system.exists(testFile), "File should exist");

        system.delete(testFile);

        assertFalse(system.exists(testFile), "File should not exist anymore");

        assertThrows(FileNotFoundException.class, () -> system.read(testFile), "Reading nonexistent file should throw exception");
    }

    @Test
    @DisplayName("When cleanup is triggered file is deleted physically")
    public void cleansUpAfterDeletion() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString(), SingleFileSystem.CleanupStrategy.CHECK_COUNT, 0.3f);

        String testString = "Testing deletion";

        String fileNameBase = "testfolder/testsubfolder/testfile";
        for (int i = 0; i < 3; i++) {

            system.write(fileNameBase + i, testString.getBytes(), true);
        }

        String testFile = fileNameBase + "1";

        system.delete(testFile);

        RandomAccessFile directAccessFile = new RandomAccessFile(SYSTEM_FILE_PATH.toString(), "r");

        long offset = 0;
        long fileLength = directAccessFile.length();
        while (offset < fileLength) {
            directAccessFile.seek(offset);
            String fileName = directAccessFile.readUTF();
            int size = directAccessFile.read();
            boolean deleted = directAccessFile.readBoolean();
            long fileOffset = directAccessFile.getFilePointer();
            assertNotEquals(testFile, fileName);
            offset = fileOffset + size;
        }

        directAccessFile.close();
    }

    @Test
    @DisplayName("When cleanup is triggered and fill rate is not reached. file is not deleted physically")
    public void checksFillRateAfterDeletion() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString(), SingleFileSystem.CleanupStrategy.CHECK_COUNT, 0.6f);

        String testString = "Testing deletion";

        String fileNameBase = "testfolder/testsubfolder/testfile";
        for (int i = 0; i < 3; i++) {

            system.write(fileNameBase + i, testString.getBytes(), true);
        }

        String testFile = fileNameBase + "1";

        system.delete(testFile);

        RandomAccessFile directAccessFile = new RandomAccessFile(SYSTEM_FILE_PATH.toString(), "r");

        int fileCount = 0;
        long offset = 0;
        long fileLength = directAccessFile.length();
        while (offset < fileLength) {
            directAccessFile.seek(offset);
            String fileName = directAccessFile.readUTF();
            int size = directAccessFile.readInt();
            boolean deleted = directAccessFile.readBoolean();
            long fileOffset = directAccessFile.getFilePointer();
            fileCount++;
            offset = fileOffset + size;
        }

        directAccessFile.close();

        assertEquals(3, fileCount, "Should find all three files");
    }

    @Test
    @DisplayName("When files of different types are added they can be read back")
    public void canLoadRealFiles() throws IOException, URISyntaxException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Path docPath = Paths.get(getClass().getClassLoader().getResource("file-sample_1MB.doc").toURI());
        byte[] docContent = Files.readAllBytes(docPath);
        system.write("testfolder/doc/sample.doc", docContent, true);

        Path mp3Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP3_5MG.mp3").toURI());
        byte[] mp3Content = Files.readAllBytes(mp3Path);
        system.write("testfolder/mp3/sample.mp3", mp3Content, true);

        Path mp4Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP4_640_3MG.mp4").toURI());
        byte[] mp4Content = Files.readAllBytes(mp4Path);
        system.write("testfolder/mp4/sample.mp4", mp4Content, true);

        Path pngPath = Paths.get(getClass().getClassLoader().getResource("file_example_PNG_2100kB.png").toURI());
        byte[] pngContent = Files.readAllBytes(pngPath);
        system.write("testfolder/png/sample.png", pngContent, true);

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        byte[] mp3bytes = system.read("testfolder/mp3/sample.mp3");

        assertArrayEquals(mp3Content, mp3bytes, "Contents should be equal");

        byte[] pngbytes = system.read("testfolder/png/sample.png");

        assertArrayEquals(pngContent, pngbytes, "Contents should be equal");

    }

    @Test
    @DisplayName("When file is loaded multiple times, it can still be retrieved")
    public void canWriteFileMultipleTimes() throws IOException, URISyntaxException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Path mp4Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP4_640_3MG.mp4").toURI());
        byte[] mp4Content = Files.readAllBytes(mp4Path);
        int fileCount = 25;
        for (int i = 0; i < 100; i++) {
            system.write("testfolder/mp4/sample_" + (i % fileCount) + ".mp4", mp4Content, true);
        }

         byte[] mp4bytes = system.read("testfolder/mp4/sample_3.mp4");

        assertArrayEquals(mp4Content, mp4bytes, "Contents should be equal");
    }

    @Test
    @DisplayName("Multiple files can be read simultaneously")
    public void canReadMultipleFiles() throws IOException, InterruptedException, ExecutionException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Test content";

        for (int i = 0; i < 100; i++) {
            system.write("testfile" + i, (testString + i).getBytes(), true);
        }

        ExecutorService executor = Executors.newFixedThreadPool(3);

        ArrayList<Callable<byte[]>> readCallables = new ArrayList<>();

        readCallables.add(() -> system.read("testfile24"));
        readCallables.add(() -> system.read("testfile97"));
        readCallables.add(() -> system.read("testfile3"));

        List<String> expectedStrings = Arrays.asList("Test content24", "Test content97", "Test content3");
        List<Future<byte[]>> futures = executor.invokeAll(readCallables);

        for (Future<byte[]> future : futures) {
            byte[] content = future.get();
            String actualString = new String(content);
            assertTrue(expectedStrings.contains(actualString), "Should read correct files");
        }
    }

    @Test
    @DisplayName("Files can be found")
    public void canFindFilesInDifferentFolders() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Test content";

        system.write("/folder1/testfile1.txt", testString.getBytes(), true);
        system.write("/folder1/testfile2.txt", testString.getBytes(), true);
        system.write("/folder2/testfile1.txt", testString.getBytes(), true);
        system.write("/folder1/subfolder1/testfile1.txt", testString.getBytes(), true);
        system.write("/folder1/subfolder2/testfile2.txt", testString.getBytes(), true);
        system.write("/folder1/subfolder2/subsubfolder1/testfile2.txt", testString.getBytes(), true);

        List<String> files = system.findFile("testfile2.txt");

        assertEquals(3, files.size(), "Should find expected number of files");
    }

    @Test
    @DisplayName("When nonexistent file is requested system continues to work properly")
    public void canWriteAfterError() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing read after error";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.write(testFile, testString.getBytes(), true);

        assertThrows(FileNotFoundException.class, () -> system.read("nonexistentfile"));

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                system.write(testFile, testString.getBytes(), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).get(50, TimeUnit.MILLISECONDS);
    }
}