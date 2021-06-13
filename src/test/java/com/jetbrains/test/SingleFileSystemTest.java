package com.jetbrains.test;

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
        system.writeFile(testFile, testString.getBytes());

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.readFile(testFile);
        assertEquals(testString, new String(bytes));
    }

    @Test
    @DisplayName("When new file is updated new version can be read back")
    public void canUpdateFile() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing write/read";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.writeFile(testFile, testString.getBytes());

        String testStringUpdated = testString + 2;
        system.writeFile(testFile, testStringUpdated.getBytes());

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.readFile(testFile);
        assertEquals(testStringUpdated, new String(bytes));
    }

    @Test
    @DisplayName("When file is deleted reading it throws FileNotFoundException")
    public void cannotFindDeleted() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Testing deletion";

        String fileNameBase = "testfolder/testsubfolder/testfile";
        for (int i = 0; i < 10; i++) {

            system.writeFile(fileNameBase + i, testString.getBytes());
        }


        String testFile = fileNameBase + "1";
        assertTrue(system.exists(testFile), "File should exist");

        system.deleteFile(testFile);

        assertFalse(system.exists(testFile), "File should not exist anymore");

        assertThrows(FileNotFoundException.class, () -> system.readFile(testFile), "Reading nonexistent file should throw exception");
    }

    @Test
    @DisplayName("When cleanup is triggered file is deleted physically")
    public void cleansUpAfterDeletion() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString(), SingleFileSystem.CleanupStrategy.CHECK_COUNT, 0.3f);

        String testString = "Testing deletion";

        String fileNameBase = "testfolder/testsubfolder/testfile";
        for (int i = 0; i < 3; i++) {

            system.writeFile(fileNameBase + i, testString.getBytes());
        }

        String testFile = fileNameBase + "1";

        system.deleteFile(testFile);

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

            system.writeFile(fileNameBase + i, testString.getBytes());
        }

        String testFile = fileNameBase + "1";

        system.deleteFile(testFile);

        RandomAccessFile directAccessFile = new RandomAccessFile(SYSTEM_FILE_PATH.toString(), "r");

        int fileCount = 0;
        long offset = 0;
        long fileLength = directAccessFile.length();
        while (offset < fileLength) {
            directAccessFile.seek(offset);
            String fileName = directAccessFile.readUTF();
            int size = directAccessFile.read();
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
        system.writeFile("testfolder/doc/sample.doc", docContent);

        Path mp3Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP3_5MG.mp3").toURI());
        byte[] mp3Content = Files.readAllBytes(mp3Path);
        system.writeFile("testfolder/mp3/sample.mp3", mp3Content);

        Path mp4Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP4_640_3MG.mp4").toURI());
        byte[] mp4Content = Files.readAllBytes(mp4Path);
        system.writeFile("testfolder/mp4/sample.mp4", mp4Content);

        Path pngPath = Paths.get(getClass().getClassLoader().getResource("file_example_PNG_2100kB.png").toURI());
        byte[] pngContent = Files.readAllBytes(pngPath);
        system.writeFile("testfolder/png/sample.png", pngContent);

        system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        byte[] mp3bytes = system.readFile("testfolder/mp3/sample.mp3");

        assertArrayEquals(mp3Content, mp3bytes, "Contents should be equal");

        byte[] pngbytes = system.readFile("testfolder/png/sample.png");

        assertArrayEquals(pngContent, pngbytes, "Contents should be equal");

    }

    @Test
    @DisplayName("When file is loaded multiple times, it can still be retrieved")
    public void canWriteFileMultipleTimes() throws IOException, URISyntaxException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        Path mp4Path = Paths.get(getClass().getClassLoader().getResource("file_example_MP4_640_3MG.mp4").toURI());
        byte[] mp4Content = Files.readAllBytes(mp4Path);
        int fileCount = 25;
        for (int i = 0; i < 1000; i++) {
            system.writeFile("testfolder/mp4/sample_" + (i % fileCount) + ".mp4", mp4Content);
        }

         byte[] mp4bytes = system.readFile("testfolder/mp4/sample_3.mp4");

        assertArrayEquals(mp4Content, mp4bytes, "Contents should be equal");
    }

    @Test
    @DisplayName("Multiple files can be read simultaneously")
    public void canReadMultipleFiles() throws IOException, InterruptedException, ExecutionException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_PATH.toString());

        String testString = "Test content";

        for (int i = 0; i < 100; i++) {
            system.writeFile("testfile" + i, (testString + i).getBytes());
        }

        ExecutorService executor = Executors.newFixedThreadPool(3);

        ArrayList<Callable<byte[]>> readCallables = new ArrayList<>();

        readCallables.add(() -> system.readFile("testfile24"));
        readCallables.add(() -> system.readFile("testfile97"));
        readCallables.add(() -> system.readFile("testfile3"));

        List<String> expectedStrings = Arrays.asList("Test content24", "Test content97", "Test content3");
        List<Future<byte[]>> futures = executor.invokeAll(readCallables);

        for (Future<byte[]> future : futures) {
            byte[] content = future.get();
            String actualString = new String(content);
            assertTrue(expectedStrings.contains(actualString), "Should read correct files");
        }
    }
}