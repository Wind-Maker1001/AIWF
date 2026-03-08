package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public final class StepDoneCallbackReq extends FlexibleBody {

    @JsonProperty("output_hash")
    private String outputHash;

    public String getOutputHash() {
        return outputHash;
    }

    public void setOutputHash(String outputHash) {
        this.outputHash = outputHash;
    }

    public Map<String, Object> payload() {
        Map<String, Object> out = new LinkedHashMap<>();
        putIfNotNull(out, "output_hash", outputHash);
        out.putAll(extras());
        return out;
    }
}
