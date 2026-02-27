package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.db.AiWfDao;
import com.aiwf.base.service.JobStatusService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CallbackController.class)
@AutoConfigureMockMvc(addFilters = false)
class CallbackControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AiWfDao dao;

    @MockBean
    private JobStatusService jobStatusService;

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

        verify(dao, never()).upsertArtifact(any(), any(), any(), any(), any(), any());
    }

    @Test
    void registerArtifactAcceptsValidPayload() throws Exception {
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

        verify(dao).upsertArtifact(eq("job1"), eq("xlsx_fin_001"), eq("xlsx"), eq("D:\\AIWF\\bus\\jobs\\x\\artifacts\\fin.xlsx"), any(), any());
    }
}
