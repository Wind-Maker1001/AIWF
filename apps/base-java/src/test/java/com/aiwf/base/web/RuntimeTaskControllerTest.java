package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.service.RuntimeTaskService;
import com.aiwf.base.web.dto.RuntimeTaskCancelResp;
import com.aiwf.base.web.dto.RuntimeTaskGetResp;
import com.aiwf.base.web.dto.RuntimeTaskListResp;
import com.aiwf.base.web.dto.RuntimeTaskResp;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
    private RuntimeTaskService tasks;

    @MockBean
    private AppProperties appProperties;

    @Test
    void upsertTaskRequiresTaskId() throws Exception {
        doThrow(ApiException.badRequest("task_id_required", "task_id required"))
                .when(tasks).upsertTask(org.mockito.ArgumentMatchers.any());

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
                .thenReturn(new RuntimeTaskGetResp(true, new RuntimeTaskResp("t1", "default", "transform_rows_v2", "done", 1L, 2L, null, null, "accel-rust")));
        mockMvc.perform(get("/api/v1/runtime/tasks/t1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.task.task_id").value("t1"));
    }

    @Test
    void getTaskReturnsNotFoundShape() throws Exception {
        when(tasks.getTask(eq("missing")))
                .thenThrow(ApiException.notFound("task_not_found", "task not found", java.util.Map.of("task_id", "missing")));

        mockMvc.perform(get("/api/v1/runtime/tasks/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.ok").value(false))
                .andExpect(jsonPath("$.error").value("task_not_found"))
                .andExpect(jsonPath("$.task_id").value("missing"));
    }

    @Test
    void cancelTaskReturnsCancelled() throws Exception {
        when(tasks.cancelTask(eq("t2")))
                .thenReturn(new RuntimeTaskCancelResp(true, "t2", true, "cancelled"));
        mockMvc.perform(post("/api/v1/runtime/tasks/t2/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.cancelled").value(true))
                .andExpect(jsonPath("$.status").value("cancelled"));
    }

    @Test
    void upsertTaskReturnsResponseFromService() throws Exception {
        when(tasks.upsertTask(org.mockito.ArgumentMatchers.any()))
                .thenReturn(new RuntimeTaskUpsertResp(true, "t9", "default", "queued"));

        mockMvc.perform(post("/api/v1/runtime/tasks/upsert")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"task_id\":\"t9\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.task_id").value("t9"))
                .andExpect(jsonPath("$.status").value("queued"));
    }

    @Test
    void listTasksReturnsResponseFromService() throws Exception {
        when(tasks.listTasksByTenant(eq("tenant-a"), eq(5)))
                .thenReturn(new RuntimeTaskListResp(
                        true,
                        "tenant-a",
                        java.util.List.of(new RuntimeTaskResp("t1", "tenant-a", "transform_rows_v2", "running", 1L, 2L, null, null, "accel-rust"))
                ));

        mockMvc.perform(get("/api/v1/runtime/tasks").param("tenant_id", "tenant-a").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenant_id").value("tenant-a"))
                .andExpect(jsonPath("$.tasks[0].task_id").value("t1"));
    }

    @Test
    void upsertTaskReturnsInvalidStatusShape() throws Exception {
        doThrow(ApiException.badRequest("runtime_task_status_invalid", "invalid runtime task status", java.util.Map.of("status", "weird")))
                .when(tasks).upsertTask(org.mockito.ArgumentMatchers.any());

        mockMvc.perform(post("/api/v1/runtime/tasks/upsert")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"task_id\":\"t9\",\"status\":\"weird\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("runtime_task_status_invalid"))
                .andExpect(jsonPath("$.status").value("weird"));
    }
}
