package com.aiwf.base.integration;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobFlowIntegrationTest extends IntegrationTestSupport {

    @Test
    void runCleaningUsesRealSqlAndGlueStub() throws Exception {
        String body = """
                {
                  "owner": "dify",
                  "actor": "dify",
                  "ruleset_version": "v2",
                  "params": {
                    "office_lang": "zh",
                    "office_theme": "debate"
                  }
                }
                """;

        String response = mockMvc.perform(post("/api/v1/integrations/dify/run_cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andReturn()
                .getResponse()
                .getContentAsString();

        Map<String, Object> out = readJsonMap(response);
        String jobId = String.valueOf(out.get("job_id"));

        assertThat(jobId).hasSize(32);
        assertThat(readJobStatus(jobId)).isEqualTo("RUNNING");
        assertThat(countRows("dbo.jobs", jobId)).isEqualTo(1);
        assertThat(Files.isDirectory(BUS_ROOT.resolve("jobs").resolve(jobId).resolve("artifacts"))).isTrue();

        List<CapturedRunRequest> runRequests = GLUE.runRequests();
        assertThat(runRequests).hasSize(1);
        assertThat(runRequests.getFirst().jobId()).isEqualTo(jobId);
        assertThat(runRequests.getFirst().flow()).isEqualTo("cleaning");
        assertThat(runRequests.getFirst().payload())
                .containsEntry("actor", "dify")
                .containsEntry("ruleset_version", "v2");
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) runRequests.getFirst().payload().get("params");
        assertThat(params)
                .containsEntry("office_lang", "zh")
                .containsEntry("office_theme", "debate");
    }

    @Test
    void stepCallbacksPersistStateAndUnknownFailDoesNotFlipJob() throws Exception {
        String createResponse = mockMvc.perform(post("/api/v1/jobs/create")
                        .param("owner", "ops")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        String jobId = String.valueOf(readJsonMap(createResponse).get("job_id"));

        mockMvc.perform(post("/api/v1/jobs/{jobId}/steps/{stepId}/start", jobId, "cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "input_uri": "bus://in.csv",
                                  "output_uri": "bus://out.csv",
                                  "ruleset_version": "v3",
                                  "params": {
                                    "sample": true
                                  }
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true));

        mockMvc.perform(post("/api/v1/jobs/{jobId}/steps/{stepId}/fail", jobId, "missing-step")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"error\":\"boom\"}"))
                .andExpect(status().isNotFound());

        assertThat(readJobStatus(jobId)).isEqualTo("RUNNING");

        mockMvc.perform(post("/api/v1/jobs/{jobId}/steps/{stepId}/done", jobId, "cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"output_hash\":\"abc123\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true));

        mockMvc.perform(get("/api/v1/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DONE"));

        List<Map<String, Object>> steps = jdbc.queryForList(
                "SELECT step_id, status, ruleset_version, output_hash FROM dbo.steps WHERE job_id = ?",
                jobId
        );
        assertThat(steps).singleElement().satisfies(step -> {
            assertThat(step).containsEntry("step_id", "cleaning");
            assertThat(step).containsEntry("status", "DONE");
            assertThat(step).containsEntry("ruleset_version", "v3");
            assertThat(step).containsEntry("output_hash", "abc123");
        });
    }

    @Test
    void glueFailureReturnsErrorAndWritesAuditLog() throws Exception {
        String createResponse = mockMvc.perform(post("/api/v1/jobs/create")
                        .param("owner", "ops")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        String jobId = String.valueOf(readJsonMap(createResponse).get("job_id"));
        GLUE.enqueueRunResponse(500, Map.of("ok", false, "error", "glue exploded"));

        String response = mockMvc.perform(post("/api/v1/jobs/{jobId}/run/{flow}", jobId, "cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "actor": "ops",
                                  "ruleset_version": "v7",
                                  "params": {
                                    "sample": true
                                  }
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.job_id").value(jobId))
                .andExpect(jsonPath("$.flow").value("cleaning"))
                .andReturn()
                .getResponse()
                .getContentAsString();

        Map<String, Object> out = readJsonMap(response);
        assertThat(String.valueOf(out.get("error"))).contains("500");
        assertThat(readJobStatus(jobId)).isEqualTo("RUNNING");

        List<Map<String, Object>> audits = jdbc.queryForList(
                "SELECT actor, action, job_id, step_id FROM dbo.audit_log WHERE job_id = ? ORDER BY audit_id ASC",
                jobId
        );
        assertThat(audits).hasSize(2);
        assertThat(audits.getLast())
                .containsEntry("actor", "ops")
                .containsEntry("action", "FLOW_RUN_FAIL")
                .containsEntry("job_id", jobId)
                .containsEntry("step_id", "cleaning");
    }
}
