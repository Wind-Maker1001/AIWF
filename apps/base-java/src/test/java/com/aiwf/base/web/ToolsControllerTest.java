package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.JobCreateResp;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ToolsController.class)
@AutoConfigureMockMvc(addFilters = false)
class ToolsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobService jobs;

    @MockBean
    private AppProperties appProperties;

    @Test
    void createJobPassesOwnerAndPolicy() throws Exception {
        when(jobs.createJob(eq("tester"), anyMap()))
                .thenReturn(new JobCreateResp("j1", "tester", "RUNNING", "D:\\AIWF\\bus\\jobs\\j1", null));

        mockMvc.perform(post("/api/v1/tools/create_job")
                        .param("owner", "tester")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"priority\":\"high\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.job_id").value("j1"))
                .andExpect(jsonPath("$.status").value("RUNNING"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> policyCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jobs).createJob(eq("tester"), policyCaptor.capture());
        assertThat(policyCaptor.getValue()).containsEntry("priority", "high");
    }

    @Test
    void createJobUsesDefaultOwnerWhenMissing() throws Exception {
        when(jobs.createJob(eq("local"), anyMap()))
                .thenReturn(new JobCreateResp("j2", "local", "RUNNING", "D:\\AIWF\\bus\\jobs\\j2", null));

        mockMvc.perform(post("/api/v1/tools/create_job")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.job_id").value("j2"));

        verify(jobs).createJob(eq("local"), anyMap());
    }

    @Test
    void createJobRejectsBlankOwner() throws Exception {
        mockMvc.perform(post("/api/v1/tools/create_job")
                        .param("owner", " ")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.ok").value(false));

        verify(jobs, never()).createJob(eq(" "), anyMap());
    }
}
