package com.jetbrains.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleReadTest {
    public static String SYSTEM_FILE_NAME = "target/test-classes/loadtestfilesystem";

    public static final int FILE_COUNT = 1;

    public static String FILE_CONTENTS = "test text file";

    @BeforeAll
    public static void initFile() throws IOException {

        Files.deleteIfExists(Paths.get(SYSTEM_FILE_NAME));

        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_NAME);

        for (int i = 0; i < FILE_COUNT; i++) {
            String fileName = "sample_" + i + ".txt";
            system.write(fileName, FILE_CONTENTS.getBytes(), true);
        }

    }

    @AfterAll
    public static void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(SYSTEM_FILE_NAME));
    }

    @Test
    public void readRandomFile() throws IOException {
        FileSystem system = SingleFileSystem.create(SYSTEM_FILE_NAME);

        int fileNumber = new Random().nextInt(FILE_COUNT);

        byte[] actualBytes = system.read("sample_" + fileNumber + ".txt");

        assertEquals(FILE_CONTENTS, new String(actualBytes), "Contents should be equal");
    }

    @Test
    public void readFileFromDisk() throws IOException, URISyntaxException {
        Path filePath = Paths.get(LoadTest.class.getClassLoader().getResource("file_sample.txt").toURI());
        byte[] actualContents = Files.readAllBytes(filePath);

        assertEquals(FILE_CONTENTS, new String(actualContents), "Contents should be equal");
    }


}
