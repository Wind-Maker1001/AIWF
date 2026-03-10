package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.service.JobCallbackService;
import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.ArtifactRegisterResp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CallbackController.class)
@AutoConfigureMockMvc(addFilters = false)
class CallbackControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobCallbackService callbacks;

    @MockBean
    private JobService jobs;

    @MockBean
    private AppProperties appProperties;

    @Test
    void registerArtifactRejectsMissingRequiredFields() throws Exception {
        String body = """
                {
                  "kind": "xlsx",
                  "path": "D:\\\\AIWF\\\\bus\\\\jobs\\\\x\\\\artifacts\\\\fin.xlsx"
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job1/artifacts/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        verify(callbacks, never()).registerArtifact(any(), any(), any());
    }

    @Test
    void registerArtifactAcceptsValidPayload() throws Exception {
        when(callbacks.registerArtifact(eq("job1"), eq("glue"), any()))
                .thenReturn(new ArtifactRegisterResp(true, "xlsx_fin_001"));

        String body = """
                {
                  "artifact_id": "xlsx_fin_001",
                  "kind": "xlsx",
                  "path": "D:\\\\AIWF\\\\bus\\\\jobs\\\\x\\\\artifacts\\\\fin.xlsx"
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job1/artifacts/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk());

        verify(callbacks).registerArtifact(eq("job1"), eq("glue"), any());
    }

    @Test
    void registerArtifactReturnsNotFoundForUnknownJob() throws Exception {
        when(callbacks.registerArtifact(eq("missing-job"), eq("glue"), any()))
                .thenThrow(ApiException.notFound("job_not_found", "job not found", java.util.Map.of("job_id", "missing-job")));

        mockMvc.perform(post("/api/v1/jobs/missing-job/artifacts/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "artifact_id": "xlsx_fin_001",
                                  "kind": "xlsx",
                                  "path": "D:\\\\AIWF\\\\bus\\\\jobs\\\\x\\\\artifacts\\\\fin.xlsx"
                                }
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void stepFailReturnsNotFoundForUnknownStep() throws Exception {
        when(jobs.failStep(eq("job1"), eq("missing-step"), eq("glue"), eq("boom"), eq("{\"error\":\"boom\"}")))
                .thenThrow(ApiException.notFound("step_not_found", "step not found", java.util.Map.of("job_id", "job1", "step_id", "missing-step")));

        mockMvc.perform(post("/api/v1/jobs/job1/steps/missing-step/fail")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"error\":\"boom\"}"))
                .andExpect(status().isNotFound());

        verify(callbacks, never()).stepDone(any(), any(), any(), any());
    }

    @Test
    void stepDoneReturnsNotFoundForUnknownStep() throws Exception {
        doThrow(ApiException.notFound("step_not_found", "step not found", java.util.Map.of("job_id", "job1", "step_id", "missing-step")))
                .when(callbacks).stepDone(eq("job1"), eq("missing-step"), eq("glue"), any());

        mockMvc.perform(post("/api/v1/jobs/job1/steps/missing-step/done")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void stepDoneReturnsConflictForFailedStep() throws Exception {
        doThrow(ApiException.conflict("step_transition_conflict", "step cannot transition to DONE", java.util.Map.of("job_id", "job1", "step_id", "step1", "current_status", "FAILED")))
                .when(callbacks).stepDone(eq("job1"), eq("step1"), eq("glue"), any());

        mockMvc.perform(post("/api/v1/jobs/job1/steps/step1/done")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isConflict());
    }

    @Test
    void stepDoneIsIdempotentWhenAlreadyDone() throws Exception {
        mockMvc.perform(post("/api/v1/jobs/job1/steps/step1/done")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"output_hash\":\"abc123\"}"))
                .andExpect(status().isOk());
        verify(callbacks).stepDone(eq("job1"), eq("step1"), eq("glue"), any());
    }

    @Test
    void stepStartReturnsConflictForDoneStep() throws Exception {
        doThrow(ApiException.conflict("step_transition_conflict", "step cannot transition to RUNNING", java.util.Map.of("job_id", "job1", "step_id", "step1", "current_status", "DONE")))
                .when(callbacks).stepStart(eq("job1"), eq("step1"), eq("glue"), any());

        mockMvc.perform(post("/api/v1/jobs/job1/steps/step1/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isConflict());
        verify(callbacks).stepStart(eq("job1"), eq("step1"), eq("glue"), any());
    }
}
