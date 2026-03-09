package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ArtifactRegisterResp(
        boolean ok,
        @JsonProperty("artifact_id") String artifactId
) {
}
