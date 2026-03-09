package com.aiwf.base.service;

import com.aiwf.base.db.RuntimeTaskRepository;
import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RuntimeTaskServiceTest {

    @Mock
    private RuntimeTaskRepository tasks;

    private RuntimeTaskService service;

    @BeforeEach
    void setUp() {
        service = new RuntimeTaskService(tasks);
    }

    @Test
    void upsertTaskAppliesDefaults() {
        RuntimeTaskUpsertReq body = new RuntimeTaskUpsertReq("t1", null, null, null, null, null, null, null, null);

        RuntimeTaskUpsertResp resp = service.upsertTask(body);

        assertThat(resp.ok()).isTrue();
        assertThat(resp.taskId()).isEqualTo("t1");
        assertThat(resp.tenantId()).isEqualTo("default");
        assertThat(resp.status()).isEqualTo("queued");

        ArgumentCaptor<Long> createdCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> updatedCap = ArgumentCaptor.forClass(Long.class);
        verify(tasks).upsertTask(
                eq("t1"),
                eq("default"),
                eq("transform_rows_v2"),
                eq("queued"),
                createdCap.capture(),
                updatedCap.capture(),
                eq(null),
                eq(null),
                eq("accel-rust")
        );
        assertThat(createdCap.getValue()).isPositive();
        assertThat(updatedCap.getValue()).isEqualTo(createdCap.getValue());
    }

    @Test
    void getTaskRejectsMissingTask() {
        when(tasks.getTask("missing")).thenReturn(null);

        assertThatThrownBy(() -> service.getTask("missing"))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("task not found");
    }

    @Test
    void cancelTaskRejectsMissingTask() {
        when(tasks.cancelTask(eq("missing"), anyLong()))
                .thenReturn(new RuntimeTaskCancelResult(false, null));

        assertThatThrownBy(() -> service.cancelTask("missing"))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("task not found");
    }

    @Test
    void listTasksMapsStoreFailureTo503() {
        when(tasks.listTasksByTenant("tenant-a", 100))
                .thenThrow(new DataAccessResourceFailureException("sql down"));

        assertThatThrownBy(() -> service.listTasksByTenant("tenant-a", 100))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("sql down");
    }

    @Test
    void getTaskReturnsResponse() {
        when(tasks.getTask("t1"))
                .thenReturn(new RuntimeTaskRow("t1", "default", "transform_rows_v2", "done", 1L, 2L, null, null, "accel-rust"));

        var resp = service.getTask("t1");

        assertThat(resp.ok()).isTrue();
        assertThat(resp.task().taskId()).isEqualTo("t1");
    }
}
