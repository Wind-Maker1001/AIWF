package com.aiwf.base.service;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class GlueRunParamsSupportTest {

    @Test
    void filterReservedKeysKeepsInsertionOrderAndDropsReservedEntries() {
        Map<String, Object> filtered = GlueRunParamsSupport.filterReservedKeys(
                Map.ofEntries(
                        Map.entry("sample", true),
                        Map.entry("job_context", Map.of("job_root", "D:\\legacy")),
                        Map.entry("trace_id", "legacy-trace"),
                        Map.entry("job_root", "D:\\legacy"),
                        Map.entry("input_rows", 3)
                ),
                Set.of("job_context", "trace_id")
        );

        assertThat(filtered)
                .containsEntry("sample", true)
                .containsEntry("job_root", "D:\\legacy")
                .containsEntry("input_rows", 3)
                .doesNotContainKeys("job_context", "trace_id");
    }

    @Test
    void filterReservedKeysReturnsEmptyMapForNullInput() {
        assertThat(GlueRunParamsSupport.filterReservedKeys(null, Set.of("job_context"))).isEmpty();
    }
}
