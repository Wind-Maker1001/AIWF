package com.aiwf.base.service;

import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeTaskServiceSupportTest {

    @Test
    void prepareUpsertAppliesDefaults() {
        var prepared = RuntimeTaskServiceSupport.prepareUpsert(
                new RuntimeTaskUpsertReq("t1", null, null, null, null, null, null, null, null, null, null),
                null,
                123L
        );

        assertThat(prepared.taskId()).isEqualTo("t1");
        assertThat(prepared.tenantId()).isEqualTo("default");
        assertThat(prepared.operator()).isEqualTo("transform_rows_v2");
        assertThat(prepared.status()).isEqualTo(RuntimeTaskStatus.QUEUED);
        assertThat(prepared.createdAtEpoch()).isEqualTo(123L);
        assertThat(prepared.updatedAtEpoch()).isEqualTo(123L);
        assertThat(prepared.source()).isEqualTo("accel-rust");
        assertThat(prepared.attempts()).isEqualTo(0);
    }

    @Test
    void applyExistingSkipsStaleUpdate() {
        var existing = new RuntimeTaskRow(
                "t1",
                "tenant-a",
                "transform_rows_v2",
                RuntimeTaskStatus.RUNNING,
                10L,
                200L,
                null,
                null,
                "accel-rust",
                "idem-1",
                2
        );
        var prepared = new RuntimeTaskServiceSupport.PreparedRuntimeTaskUpsert(
                "t1",
                "tenant-a",
                "transform_rows_v2",
                RuntimeTaskStatus.QUEUED,
                10L,
                150L,
                null,
                null,
                "accel-rust",
                "idem-1",
                1
        );

        var resolution = RuntimeTaskServiceSupport.applyExisting(existing, prepared);

        assertThat(resolution.shouldProceed()).isFalse();
        assertThat(resolution.immediateResponse().status()).isEqualTo("running");
    }

    @Test
    void applyExistingRejectsTenantRewrite() {
        var existing = new RuntimeTaskRow(
                "t1",
                "tenant-a",
                "transform_rows_v2",
                RuntimeTaskStatus.RUNNING,
                10L,
                20L,
                null,
                null,
                "accel-rust",
                "idem-1",
                2
        );
        var prepared = new RuntimeTaskServiceSupport.PreparedRuntimeTaskUpsert(
                "t1",
                "tenant-b",
                "transform_rows_v2",
                RuntimeTaskStatus.RUNNING,
                10L,
                30L,
                null,
                null,
                "accel-rust",
                "idem-1",
                2
        );

        assertThatThrownBy(() -> RuntimeTaskServiceSupport.applyExisting(existing, prepared))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("tenant cannot change");
    }
}
