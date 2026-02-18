package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.db.AiWfDao;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RuntimeTaskController.class)
@AutoConfigureMockMvc(addFilters = false)
class RuntimeTaskControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AiWfDao dao;

    @MockBean
    private AppProperties appProperties;

    @Test
    void upsertTaskRequiresTaskId() throws Exception {
        mockMvc.perform(post("/api/v1/runtime/tasks/upsert")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"queued\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(false));
    }

    @Test
    void getTaskReturnsTask() throws Exception {
        when(dao.getWorkflowTask(eq("t1")))
                .thenReturn(Map.of("task_id", "t1", "status", "done", "operator", "transform_rows_v2"));
        mockMvc.perform(get("/api/v1/runtime/tasks/t1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.task.task_id").value("t1"));
    }

    @Test
    void cancelTaskReturnsCancelled() throws Exception {
        when(dao.cancelWorkflowTask(eq("t2"), anyLong()))
                .thenReturn(Map.of("cancelled", true, "task", Map.of("status", "cancelled")));
        mockMvc.perform(post("/api/v1/runtime/tasks/t2/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.cancelled").value(true))
                .andExpect(jsonPath("$.status").value("cancelled"));
    }
}
