package com.aiwf.base;

import com.aiwf.base.config.DotenvLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
public class BaseApplication {
    public static void main(String[] args) {
        String envFile = System.getProperty("aiwf.env.file");
        if (envFile == null || envFile.isBlank()) {
            envFile = System.getenv("AIWF_ENV_FILE");
        }
        if (envFile == null || envFile.isBlank()) {
            envFile = detectDefaultEnvFile();
        }

        DotenvLoader.load(envFile);
        SpringApplication.run(BaseApplication.class, args);
    }

    private static String detectDefaultEnvFile() {
        Path cwd = Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        Path[] candidates = new Path[] {
                cwd.resolve("ops").resolve("config").resolve("dev.env"),
                cwd.resolve("..").resolve("..").resolve("ops").resolve("config").resolve("dev.env").normalize(),
                Paths.get("ops", "config", "dev.env").toAbsolutePath().normalize()
        };
        for (Path p : candidates) {
            if (Files.isRegularFile(p)) {
                return p.toString();
            }
        }
        return candidates[0].toString();
    }
}
