package com.aiwf.base.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;

class DotenvLoaderTest {

    @Test
    void loadIgnoresNullPath() {
        assertThatCode(() -> DotenvLoader.load(null)).doesNotThrowAnyException();
    }

    @Test
    void loadParsesSimpleEnvFile() throws Exception {
        Path envFile = Files.createTempFile("aiwf-dotenv", ".env");
        String key = "AIWF_DOTENV_TEST_" + System.nanoTime();
        Files.writeString(envFile, key + "=hello\n");
        try {
            DotenvLoader.load(envFile.toString());
            org.assertj.core.api.Assertions.assertThat(System.getProperty(key)).isEqualTo("hello");
        } finally {
            System.clearProperty(key);
            Files.deleteIfExists(envFile);
        }
    }
}
