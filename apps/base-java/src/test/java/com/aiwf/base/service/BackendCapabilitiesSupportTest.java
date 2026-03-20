package com.aiwf.base.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BackendCapabilitiesSupportTest {

    @Test
    void unavailableSnapshotFallsBackWhenMessageBlank() {
        assertThat(BackendCapabilitiesSupport.unavailableSnapshot(" ", "fallback"))
                .containsEntry("ok", false)
                .containsEntry("error", "fallback");
    }

    @Test
    void buildSnapshotAggregatesDomainArrays() {
        Map<String, Object> glueCaps = Map.of(
                "ok", true,
                "capabilities", Map.of(
                        "flow_domains", List.of(Map.of("name", "cleaning"))
                )
        );
        Map<String, Object> accelCaps = Map.of(
                "ok", true,
                "domains", List.of(Map.of("name", "governance")),
                "workflow_domains", List.of(Map.of("name", "transform"))
        );

        Map<String, Object> snapshot = BackendCapabilitiesSupport.buildSnapshot(
                "http://glue",
                glueCaps,
                "http://accel",
                accelCaps
        );

        assertThat(snapshot).containsEntry("ok", true);
        assertThat(((Map<?, ?>) snapshot.get("glue")).get("url")).isEqualTo("http://glue");
        assertThat(((Map<?, ?>) snapshot.get("accel")).get("url")).isEqualTo("http://accel");
        assertThat(((Map<?, ?>) snapshot.get("domains")).get("flow_domains")).asList().hasSize(1);
        assertThat(((Map<?, ?>) snapshot.get("domains")).get("published_operator_domains")).asList().hasSize(1);
        assertThat(((Map<?, ?>) snapshot.get("domains")).get("workflow_operator_domains")).asList().hasSize(1);
    }
}
