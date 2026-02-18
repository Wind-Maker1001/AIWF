package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.config.AppProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
                .thenReturn(Map.of("ok", true, "job_id", "job1"));

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
                .thenReturn(Map.of("ok", true, "job_id", "job2"));

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
}
