package com.aiwf.base.integration;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class RuntimeTaskIntegrationTest extends IntegrationTestSupport {

    @Test
    void runtimeTaskLifecyclePersistsInSqlServer() throws Exception {
        mockMvc.perform(post("/api/v1/runtime/tasks/upsert")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "task_id": "rt-1",
                                  "tenant_id": "tenant-a",
                                  "operator": "transform_rows_v2",
                                  "status": "running",
                                  "created_at": 1710000000,
                                  "updated_at": 1710000001,
                                  "idempotency_key": "idem-rt-1",
                                  "attempts": 3,
                                  "result": {
                                    "rows": 12
                                  }
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.tenant_id").value("tenant-a"));

        mockMvc.perform(get("/api/v1/runtime/tasks/rt-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.task.task_id").value("rt-1"))
                .andExpect(jsonPath("$.task.tenant_id").value("tenant-a"))
                .andExpect(jsonPath("$.task.idempotency_key").value("idem-rt-1"))
                .andExpect(jsonPath("$.task.attempts").value(3));

        mockMvc.perform(get("/api/v1/runtime/tasks").param("tenant_id", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks[0].task_id").value("rt-1"));

        mockMvc.perform(post("/api/v1/runtime/tasks/rt-1/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cancelled").value(true))
                .andExpect(jsonPath("$.status").value("cancelled"));

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT task_id, tenant_id, status, idempotency_key, attempts FROM dbo.workflow_tasks WHERE task_id = ?",
                "rt-1"
        );
        assertThat(row).containsEntry("task_id", "rt-1");
        assertThat(row).containsEntry("tenant_id", "tenant-a");
        assertThat(row).containsEntry("status", "cancelled");
        assertThat(row).containsEntry("idempotency_key", "idem-rt-1");
        assertThat(row).containsEntry("attempts", 3);
    }
}
