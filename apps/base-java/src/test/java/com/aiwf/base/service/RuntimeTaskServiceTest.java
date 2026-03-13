package com.aiwf.base.service;

import com.aiwf.base.db.RuntimeTaskRepository;
import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
        RuntimeTaskUpsertReq body = new RuntimeTaskUpsertReq("t1", null, null, null, null, null, null, null, null, null, null);
        when(tasks.getTask("t1")).thenReturn(null);

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
                eq(RuntimeTaskStatus.QUEUED),
                createdCap.capture(),
                updatedCap.capture(),
                eq(null),
                eq(null),
                eq("accel-rust"),
                eq(null),
                eq(0)
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
                .thenReturn(new RuntimeTaskRow("t1", "default", "transform_rows_v2", RuntimeTaskStatus.DONE, 1L, 2L, null, null, "accel-rust", "idem-1", 2));

        var resp = service.getTask("t1");

        assertThat(resp.ok()).isTrue();
        assertThat(resp.task().taskId()).isEqualTo("t1");
        assertThat(resp.task().idempotencyKey()).isEqualTo("idem-1");
        assertThat(resp.task().attempts()).isEqualTo(2);
    }

    @Test
    void upsertTaskRejectsInvalidStatus() {
        RuntimeTaskUpsertReq body = new RuntimeTaskUpsertReq("t1", null, "weird", null, null, null, null, null, null, null, null);

        assertThatThrownBy(() -> service.upsertTask(body))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("invalid runtime task status");
    }

    @Test
    void upsertTaskAcceptsFailedStatus() {
        RuntimeTaskUpsertReq body = new RuntimeTaskUpsertReq("t1", null, "failed", 100L, 120L, null, "boom", null, "tenant-a", "idem-2", 4);
        when(tasks.getTask("t1")).thenReturn(null);

        RuntimeTaskUpsertResp resp = service.upsertTask(body);

        assertThat(resp.ok()).isTrue();
        assertThat(resp.status()).isEqualTo("failed");
        verify(tasks).upsertTask(
                eq("t1"),
                eq("tenant-a"),
                eq("transform_rows_v2"),
                eq(RuntimeTaskStatus.FAILED),
                eq(100L),
                eq(120L),
                eq(null),
                eq("boom"),
                eq("accel-rust"),
                eq("idem-2"),
                eq(4)
        );
    }

    @Test
    void upsertTaskIgnoresStaleRegression() {
        when(tasks.getTask("t1"))
                .thenReturn(new RuntimeTaskRow("t1", "tenant-a", "transform_rows_v2", RuntimeTaskStatus.RUNNING, 100L, 200L, null, null, "accel-rust", "idem-1", 2));

        RuntimeTaskUpsertResp resp = service.upsertTask(
                new RuntimeTaskUpsertReq("t1", "transform_rows_v2", "queued", 100L, 150L, null, null, "accel-rust", "tenant-a", "idem-1", 1)
        );

        assertThat(resp.status()).isEqualTo("running");
        verify(tasks, never()).upsertTask(any(), any(), any(), any(), anyLong(), anyLong(), any(), any(), any(), any(), anyInt());
    }

    @Test
    void upsertTaskRejectsTenantRewriteForExistingTask() {
        when(tasks.getTask("t1"))
                .thenReturn(new RuntimeTaskRow("t1", "tenant-a", "transform_rows_v2", RuntimeTaskStatus.RUNNING, 100L, 200L, null, null, "accel-rust", "idem-1", 2));

        assertThatThrownBy(() -> service.upsertTask(
                new RuntimeTaskUpsertReq("t1", "transform_rows_v2", "running", 100L, 220L, null, null, "accel-rust", "tenant-b", "idem-1", 2)
        ))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("tenant cannot change");
    }

    @Test
    void upsertTaskReturnsExistingTaskForIdempotencyHit() {
        when(tasks.getTask("t2")).thenReturn(null);
        when(tasks.getTaskByTenantOperatorAndIdempotencyKey("tenant-a", "transform_rows_v2", "idem-1"))
                .thenReturn(new RuntimeTaskRow("t-existing", "tenant-a", "transform_rows_v2", RuntimeTaskStatus.DONE, 100L, 200L, null, null, "accel-rust", "idem-1", 2));

        RuntimeTaskUpsertResp resp = service.upsertTask(
                new RuntimeTaskUpsertReq("t2", "transform_rows_v2", "queued", 100L, 220L, null, null, "accel-rust", "tenant-a", "idem-1", 0)
        );

        assertThat(resp.taskId()).isEqualTo("t-existing");
        assertThat(resp.status()).isEqualTo("done");
        verify(tasks, never()).upsertTask(any(), any(), any(), any(), anyLong(), anyLong(), any(), any(), any(), any(), anyInt());
    }
}
