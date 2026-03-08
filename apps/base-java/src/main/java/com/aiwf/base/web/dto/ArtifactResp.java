package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ArtifactResp(
        @JsonProperty("artifact_id") String artifactId,
        String kind,
        String path,
        String sha256,
        @JsonProperty("created_at") Object createdAt
) {
}
