package com.aiwf.base.service;

import com.aiwf.base.glue.GlueJobContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class JobWorkspaceSupport {
    private JobWorkspaceSupport() {
    }

    static void ensureJobDirs(String jobsBusRoot, String jobId) {
        Path jobRoot = jobRoot(jobsBusRoot, jobId);
        try {
            Files.createDirectories(jobRoot);
            Files.createDirectories(jobRoot.resolve("stage"));
            Files.createDirectories(jobRoot.resolve("artifacts"));
            Files.createDirectories(jobRoot.resolve("evidence"));
        } catch (Exception e) {
            throw new IllegalStateException("failed to prepare job directories for " + jobId, e);
        }
    }

    static String jobsRoot(String jobsBusRoot) {
        return Paths.get(jobsBusRoot, "jobs").toString();
    }

    static GlueJobContext buildJobContext(String jobsBusRoot, String jobId) {
        Path jobRoot = jobRoot(jobsBusRoot, jobId);
        return new GlueJobContext(
                jobRoot.toString(),
                jobRoot.resolve("stage").toString(),
                jobRoot.resolve("artifacts").toString(),
                jobRoot.resolve("evidence").toString()
        );
    }

    private static Path jobRoot(String jobsBusRoot, String jobId) {
        return Paths.get(jobsRoot(jobsBusRoot), jobId);
    }
}
