package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record JobTimelineItemResp(
        @JsonProperty("node_id") String nodeId,
        String type,
        String status,
        @JsonProperty("started_at") Object startedAt,
        @JsonProperty("ended_at") Object endedAt,
        double seconds,
        String error
) {
}
