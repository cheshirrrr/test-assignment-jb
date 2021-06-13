package com.jetbrains.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class GzippedFileSystemTest {

    public static final String FILE_SYSTEM_FILE = "./testfile";

    @BeforeAll
    public static void deleteFile() throws IOException {
        Path path = Paths.get(FILE_SYSTEM_FILE);
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    @AfterEach
    public void cleanUp() throws IOException {
        deleteFile();
    }

    @Test
    @DisplayName("When new file is added it can be read back")
    public void canCreateFile() throws IOException {
        FileSystem system = new GzippedFileSystem(SingleFileSystem.create(FILE_SYSTEM_FILE));

        String testString = "Testing write/read";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.writeFile(testFile, testString.getBytes());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.readFile(testFile);
        assertEquals(testString, new String(bytes));
    }

    @Test
    @DisplayName("When new file is updated new version can be read back")
    public void canUpdateFile() throws IOException {
        FileSystem system = SingleFileSystem.create(FILE_SYSTEM_FILE);

        String testString = "Testing write/read";

        String testFile = "testfolder/testsubfolder/testfile1";
        system.writeFile(testFile, testString.getBytes());

        String testStringUpdated = testString + 2;
        system.writeFile(testFile, testStringUpdated.getBytes());

        Set<String> files = system.listFiles("testfolder/testsubfolder");

        assertThat("List contains file", files.contains(testFile));

        byte[] bytes = system.readFile(testFile);
        assertEquals(testStringUpdated, new String(bytes));
    }


    @Test
    @DisplayName("When file is deleted reading it throws FileNotFoundException")
    public void cannotFindDeleted() throws IOException {
        FileSystem system = new GzippedFileSystem(SingleFileSystem.create(FILE_SYSTEM_FILE, SingleFileSystem.CleanupStrategy.CHECK_SIZE, 0.9f));

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
}