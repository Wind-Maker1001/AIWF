package com.aiwf.base.service;

import com.aiwf.base.glue.GlueJobContext;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class JobWorkspaceSupportTest {

    @Test
    void ensureJobDirsCreatesExpectedLayoutAndContext() throws Exception {
        Path busRoot = Files.createTempDirectory("aiwf-job-workspace");

        JobWorkspaceSupport.ensureJobDirs(busRoot.toString(), "job-1");

        Path jobsRoot = busRoot.resolve("jobs");
        Path jobRoot = jobsRoot.resolve("job-1");
        assertThat(Files.isDirectory(jobRoot)).isTrue();
        assertThat(Files.isDirectory(jobRoot.resolve("stage"))).isTrue();
        assertThat(Files.isDirectory(jobRoot.resolve("artifacts"))).isTrue();
        assertThat(Files.isDirectory(jobRoot.resolve("evidence"))).isTrue();

        assertThat(JobWorkspaceSupport.jobsRoot(busRoot.toString())).isEqualTo(jobsRoot.toString());

        GlueJobContext context = JobWorkspaceSupport.buildJobContext(busRoot.toString(), "job-1");
        assertThat(context.jobRoot()).isEqualTo(jobRoot.toString());
        assertThat(context.stageDir()).isEqualTo(jobRoot.resolve("stage").toString());
        assertThat(context.artifactsDir()).isEqualTo(jobRoot.resolve("artifacts").toString());
        assertThat(context.evidenceDir()).isEqualTo(jobRoot.resolve("evidence").toString());
    }
}
