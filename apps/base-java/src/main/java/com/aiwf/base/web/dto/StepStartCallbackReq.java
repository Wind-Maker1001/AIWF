package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public final class StepStartCallbackReq extends FlexibleBody {

    @JsonProperty("input_uri")
    private String inputUri;

    @JsonProperty("output_uri")
    private String outputUri;

    @JsonProperty("ruleset_version")
    private String rulesetVersion;

    @JsonProperty("params")
    private Object params;

    public String getInputUri() {
        return inputUri;
    }

    public void setInputUri(String inputUri) {
        this.inputUri = inputUri;
    }

    public String getOutputUri() {
        return outputUri;
    }

    public void setOutputUri(String outputUri) {
        this.outputUri = outputUri;
    }

    public String getRulesetVersion() {
        return rulesetVersion;
    }

    public void setRulesetVersion(String rulesetVersion) {
        this.rulesetVersion = rulesetVersion;
    }

    public Object getParams() {
        return params;
    }

    public void setParams(Object params) {
        this.params = params;
    }

    public Map<String, Object> payload() {
        Map<String, Object> out = new LinkedHashMap<>();
        putIfNotNull(out, "input_uri", inputUri);
        putIfNotNull(out, "output_uri", outputUri);
        putIfNotNull(out, "ruleset_version", rulesetVersion);
        putIfNotNull(out, "params", params);
        out.putAll(extras());
        return out;
    }
}
