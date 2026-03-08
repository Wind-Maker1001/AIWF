package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.util.LinkedHashMap;
import java.util.Map;

public record ArtifactRegisterReq(
        @JsonProperty("artifact_id") @NotBlank String artifactId,
        @JsonProperty("kind") @NotBlank String kind,
        @JsonProperty("path") @NotBlank String path,
        @JsonProperty("sha256") String sha256,
        @JsonProperty("binding_json") Object bindingJson
) {
    public Map<String, Object> payload() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("artifact_id", artifactId);
        out.put("kind", kind);
        out.put("path", path);
        if (sha256 != null) {
            out.put("sha256", sha256);
        }
        if (bindingJson != null) {
            out.put("binding_json", bindingJson);
        }
        return out;
    }
}
