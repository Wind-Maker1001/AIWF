package com.aiwf.base.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class DotenvLoader {
    private static final Logger log = LoggerFactory.getLogger(DotenvLoader.class);

    private DotenvLoader() {}

    public static void load(String path) {
        if (path == null || path.isBlank()) {
            return;
        }
        try {
            File f = new File(path);
            if (!f.exists()) return;

            try (var br = new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) continue;

                    int idx = line.indexOf('=');
                    if (idx <= 0) continue;

                    String key = line.substring(0, idx).trim();
                    String val = line.substring(idx + 1).trim();

                    // Strip optional surrounding quotes.
                    if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
                        val = val.substring(1, val.length() - 1);
                    }

                    // Do not override process/system environment values.
                    if (System.getProperty(key) == null && System.getenv(key) == null) {
                        System.setProperty(key, val);
                    }
                }
            }
        } catch (Exception e) {
            // Keep startup resilient even when dev.env is invalid.
            log.warn("failed to load dev.env path={} error={}", path, safeMessage(e));
        }
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
