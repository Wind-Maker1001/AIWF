package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.web.dto.ArtifactResp;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.JobCreateResp;
import com.aiwf.base.web.dto.StepResp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DifyController.class)
@AutoConfigureMockMvc(addFilters = false)
class DifyControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobService jobs;

    @MockBean
    private AppProperties appProperties;

    @Test
    void runCleaningOneShotReturnsJobRunAndArtifacts() throws Exception {
        when(jobs.createJob(eq("dify"), anyMap()))
                .thenReturn(new JobCreateResp("j100", "dify", "RUNNING", "D:\\AIWF\\bus\\jobs\\j100", null));
        when(jobs.runFlow(eq("j100"), eq("cleaning"), eq("dify"), eq("v1"), anyMap()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "j100"), "j100", "cleaning"));
        when(jobs.listSteps(eq("j100"))).thenReturn(List.of(new StepResp("j100", "cleaning", "DONE", null, null, null, null, null, null, null, null)));
        when(jobs.listArtifacts(eq("j100"))).thenReturn(List.of(new ArtifactResp("xlsx_fin_001", "xlsx", null, null, null)));

        String body = """
                {
                  "owner": "dify",
                  "actor": "dify",
                  "ruleset_version": "v1",
                  "params": {
                    "office_lang": "zh",
                    "office_theme": "debate"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/integrations/dify/run_cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.job_id").value("j100"))
                .andExpect(jsonPath("$.run.ok").value(true))
                .andExpect(jsonPath("$.artifacts[0].kind").value("xlsx"));
    }

    @Test
    void healthEndpointWorks() throws Exception {
        mockMvc.perform(get("/api/v1/integrations/dify/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.integration").value("dify"));
    }

    @Test
    void runCleaningNullStringsFallbackToDefaults() throws Exception {
        when(jobs.createJob(eq("dify"), anyMap()))
                .thenReturn(new JobCreateResp("j101", "dify", "RUNNING", "D:\\AIWF\\bus\\jobs\\j101", null));
        when(jobs.runFlow(eq("j101"), eq("cleaning"), eq("dify"), eq("v1"), anyMap()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "j101"), "j101", "cleaning"));
        when(jobs.listSteps(eq("j101"))).thenReturn(List.of());
        when(jobs.listArtifacts(eq("j101"))).thenReturn(List.of());

        String body = """
                {
                  "owner": null,
                  "actor": "null",
                  "ruleset_version": ""
                }
                """;

        mockMvc.perform(post("/api/v1/integrations/dify/run_cleaning")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.job_id").value("j101"));

        verify(jobs).createJob(eq("dify"), anyMap());
        verify(jobs).runFlow(eq("j101"), eq("cleaning"), eq("dify"), eq("v1"), anyMap());
    }
}
