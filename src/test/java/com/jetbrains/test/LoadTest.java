package com.jetbrains.test;

import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.domain.TestMappings;
import org.jsmart.zerocode.jupiter.extension.ParallelLoadExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Performs load test using ZeroCode library.
 * Allows to roughly judge if performance is comparable to reading files from disk
 */
@LoadWith("load_generation.properties")
@ExtendWith({ParallelLoadExtension.class})
public class LoadTest {

    @Test
    @DisplayName("Testing performance of reading from system vs reading from disk")
    @TestMappings({
            @TestMapping(testClass = SimpleReadTest.class, testMethod = "readRandomFile"),
            @TestMapping(testClass = SimpleReadTest.class, testMethod = "readFileFromDisk"),
    })
    public void testLoad() {
        // This space remains empty
    }
}
