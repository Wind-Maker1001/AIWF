package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.db.RuntimeTaskRepository;
import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

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
    private RuntimeTaskRepository tasks;

    @MockBean
    private AppProperties appProperties;

    @Test
    void upsertTaskRequiresTaskId() throws Exception {
        mockMvc.perform(post("/api/v1/runtime/tasks/upsert")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"queued\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("task_id_required"));
    }

    @Test
    void getTaskReturnsTask() throws Exception {
        when(tasks.getTask(eq("t1")))
                .thenReturn(new RuntimeTaskRow("t1", "default", "transform_rows_v2", "done", 1L, 2L, null, null, "accel-rust"));
        mockMvc.perform(get("/api/v1/runtime/tasks/t1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.task.task_id").value("t1"));
    }

    @Test
    void getTaskReturnsNotFoundShape() throws Exception {
        when(tasks.getTask(eq("missing"))).thenReturn(null);

        mockMvc.perform(get("/api/v1/runtime/tasks/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("task_not_found"))
                .andExpect(jsonPath("$.task_id").value("missing"));
    }

    @Test
    void cancelTaskReturnsCancelled() throws Exception {
        when(tasks.cancelTask(eq("t2"), anyLong()))
                .thenReturn(new RuntimeTaskCancelResult(true, new RuntimeTaskRow("t2", "default", "transform_rows_v2", "cancelled", 1L, 2L, null, null, "accel-rust")));
        mockMvc.perform(post("/api/v1/runtime/tasks/t2/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.cancelled").value(true))
                .andExpect(jsonPath("$.status").value("cancelled"));
    }
}
