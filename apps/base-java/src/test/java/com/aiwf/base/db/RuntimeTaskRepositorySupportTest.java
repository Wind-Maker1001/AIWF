package com.aiwf.base.db;

import com.aiwf.base.db.model.RuntimeTaskStatus;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RuntimeTaskRepositorySupportTest {

    @Test
    void runtimeTaskRowMapperMapsExpectedFields() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("task_id")).thenReturn("task-1");
        when(rs.getString("tenant_id")).thenReturn("tenant-a");
        when(rs.getString("operator")).thenReturn("transform_rows_v2");
        when(rs.getString("status")).thenReturn("running");
        when(rs.getLong("created_at_epoch")).thenReturn(100L);
        when(rs.getLong("updated_at_epoch")).thenReturn(200L);
        when(rs.getString("result_json")).thenReturn("{\"ok\":true}");
        when(rs.getString("error")).thenReturn(null);
        when(rs.getString("source")).thenReturn("accel-rust");
        when(rs.getString("idempotency_key")).thenReturn("idem-1");
        when(rs.getInt("attempts")).thenReturn(2);

        var row = RuntimeTaskRepositorySupport.RUNTIME_TASK_ROW_MAPPER.mapRow(rs, 0);

        assertThat(row).isNotNull();
        assertThat(row.taskId()).isEqualTo("task-1");
        assertThat(row.tenantId()).isEqualTo("tenant-a");
        assertThat(row.operator()).isEqualTo("transform_rows_v2");
        assertThat(row.status()).isEqualTo(RuntimeTaskStatus.RUNNING);
        assertThat(row.createdAtEpoch()).isEqualTo(100L);
        assertThat(row.updatedAtEpoch()).isEqualTo(200L);
        assertThat(row.resultJson()).isEqualTo("{\"ok\":true}");
        assertThat(row.source()).isEqualTo("accel-rust");
        assertThat(row.idempotencyKey()).isEqualTo("idem-1");
        assertThat(row.attempts()).isEqualTo(2);
    }
}
