package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.config.AppProperties;
import com.aiwf.base.glue.GlueHealthResult;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.dto.AuditEventResp;
import com.aiwf.base.web.dto.JobFailureSummaryResp;
import com.aiwf.base.web.dto.JobRunRecordResp;
import com.aiwf.base.web.dto.JobRunTimelineResp;
import com.aiwf.base.web.dto.JobTimelineItemResp;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
@AutoConfigureMockMvc(addFilters = false)
class JobControllerContractTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobService jobs;

    @MockBean
    private AppProperties appProperties;

    @Test
    void runFlowPassesNestedParamsAndRulesetVersion() throws Exception {
        when(jobs.runFlow(eq("job1"), eq("cleaning"), eq("local"), eq("v2"), anyMap()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "job1"), "job1", "cleaning"));

        String body = """
                {
                  "actor": "local",
                  "ruleset_version": "v2",
                  "params": {
                    "office_theme": "debate",
                    "office_lang": "zh",
                    "report_title": "作业报告"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job1/run/cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.job_id").value("job1"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> paramsCap = ArgumentCaptor.forClass(Map.class);
        verify(jobs).runFlow(eq("job1"), eq("cleaning"), eq("local"), eq("v2"), paramsCap.capture());

        Map<String, Object> sent = paramsCap.getValue();
        assertThat(sent).containsEntry("office_theme", "debate");
        assertThat(sent).containsEntry("office_lang", "zh");
        assertThat(sent).containsEntry("report_title", "作业报告");
    }

    @Test
    void runFlowSupportsLegacyTopLevelParams() throws Exception {
        when(jobs.runFlow(eq("job2"), eq("cleaning"), eq("local"), eq("v1"), anyMap()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "job2"), "job2", "cleaning"));

        String body = """
                {
                  "actor": "local",
                  "ruleset_version": "v1",
                  "office_theme": "academic",
                  "office_lang": "en"
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job2/run/cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.job_id").value("job2"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> paramsCap = ArgumentCaptor.forClass(Map.class);
        verify(jobs).runFlow(eq("job2"), eq("cleaning"), eq("local"), eq("v1"), paramsCap.capture());
        assertThat(paramsCap.getValue()).containsEntry("office_theme", "academic");
        assertThat(paramsCap.getValue()).containsEntry("office_lang", "en");
    }

    @Test
    void runReferencePassesReferenceFirstParams() throws Exception {
        when(jobs.runWorkflowReference(eq("job3"), eq("ver_finance_001"), eq("local"), eq("v3"), anyMap()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "job3", "version_id", "ver_finance_001"), "job3", ""));

        String body = """
                {
                  "version_id": "ver_finance_001",
                  "actor": "local",
                  "ruleset_version": "v3",
                  "params": {
                    "region": "cn"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job3/run-reference")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.job_id").value("job3"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> paramsCap = ArgumentCaptor.forClass(Map.class);
        verify(jobs).runWorkflowReference(eq("job3"), eq("ver_finance_001"), eq("local"), eq("v3"), paramsCap.capture());
        assertThat(paramsCap.getValue()).containsEntry("region", "cn");
    }

    @Test
    void runReferenceRejectsWorkflowPayloadFields() throws Exception {
        String body = """
                {
                  "version_id": "ver_finance_001",
                  "workflow_definition": {
                    "workflow_id": "wf_finance"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job4/run-reference")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("workflow_reference_request_invalid"));
    }

    @Test
    void legacyRunRejectsReferenceFirstFields() throws Exception {
        String body = """
                {
                  "version_id": "ver_finance_001"
                }
                """;

        mockMvc.perform(post("/api/v1/jobs/job5/run/cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("legacy_flow_not_allowed"));
    }

    @Test
    void legacyRunRejectsNonWhitelistedFlow() throws Exception {
        mockMvc.perform(post("/api/v1/jobs/job6/run/transforming")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"actor\":\"local\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("legacy_flow_not_allowed"));
    }

    @Test
    void glueHealthReflectsDownstreamFailure() throws Exception {
        when(jobs.glueHealth()).thenReturn(GlueHealthResult.unavailable("connection refused"));

        mockMvc.perform(get("/api/v1/jobs/glue/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.glue.error").value("connection refused"));
    }

    @Test
    void jobReadReturnsStructured503WhenStoreUnavailable() throws Exception {
        when(jobs.getJob(eq("job-db-down"))).thenThrow(new DataSourceLookupFailureException("sql unavailable"));

        mockMvc.perform(get("/api/v1/jobs/job-db-down"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("data_store_unavailable"));
    }

    @Test
    void jobReadReturnsStructured500ForUnexpectedError() throws Exception {
        when(jobs.getJob(eq("job-crash"))).thenThrow(new IllegalStateException("boom"));

        mockMvc.perform(get("/api/v1/jobs/job-crash"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("internal_error"));
    }

    @Test
    void listStepsReturns404ForUnknownJob() throws Exception {
        when(jobs.listSteps(eq("missing-job")))
                .thenThrow(ApiException.notFound("job_not_found", "job not found", java.util.Map.of("job_id", "missing-job")));

        mockMvc.perform(get("/api/v1/jobs/missing-job/steps"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("job_not_found"));
    }

    @Test
    void historyEndpointsExposeLifecycleRunAuditViews() throws Exception {
        when(jobs.listRunHistory(50)).thenReturn(List.of(
                new JobRunRecordResp(
                        "lifecycle_run_record.v1",
                        "base-java",
                        "base-java.jobs",
                        "job1",
                        "2026-03-28T00:00:00Z",
                        "reference",
                        "ver_finance_001",
                        "ver_finance_001",
                        "version_reference",
                        "cleaning",
                        "DONE",
                        true,
                        Map.of(),
                        Map.of(),
                        Map.of("job_id", "job1")
                )
        ));
        when(jobs.getRunTimeline("job1")).thenReturn(
                new JobRunTimelineResp(
                        "lifecycle_run_timeline.v1",
                        true,
                        "base-java",
                        "base-java.jobs",
                        "job1",
                        "DONE",
                        List.of(new JobTimelineItemResp("cleaning", "cleaning", "DONE", "2026-03-28T00:00:00Z", "2026-03-28T00:00:01Z", 1.0, ""))
                )
        );
        when(jobs.getFailureSummary(25)).thenReturn(
                new JobFailureSummaryResp(
                        "lifecycle_failure_summary.v1",
                        true,
                        "base-java",
                        "base-java.jobs",
                        10,
                        2,
                        Map.of("cleaning", Map.of("failed", 2))
                )
        );
        when(jobs.listAuditEvents(40, "STEP_DONE")).thenReturn(List.of(
                new AuditEventResp(
                        "lifecycle_audit_event.v1",
                        "base-java",
                        "base-java.jobs",
                        "2026-03-28T00:00:02Z",
                        "glue",
                        "STEP_DONE",
                        "job1",
                        "cleaning",
                        Map.of("ok", true)
                )
        ));

        mockMvc.perform(get("/api/v1/jobs/history").param("limit", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].run_id").value("job1"))
                .andExpect(jsonPath("$[0].owner").value("base-java"))
                .andExpect(jsonPath("$[0].run_request_kind").value("reference"))
                .andExpect(jsonPath("$[0].version_id").value("ver_finance_001"));

        mockMvc.perform(get("/api/v1/jobs/job1/timeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run_id").value("job1"))
                .andExpect(jsonPath("$.timeline[0].node_id").value("cleaning"));

        mockMvc.perform(get("/api/v1/jobs/failure-summary").param("limit", "25"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.failed_runs").value(2))
                .andExpect(jsonPath("$.owner").value("base-java"));

        mockMvc.perform(get("/api/v1/jobs/audit-events").param("limit", "40").param("action", "STEP_DONE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].action").value("STEP_DONE"))
                .andExpect(jsonPath("$[0].owner").value("base-java"));
    }
}
