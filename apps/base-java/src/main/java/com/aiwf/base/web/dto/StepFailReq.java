package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record StepFailReq(
        @JsonProperty("error_summary") String errorSummary,
        @JsonProperty("error") String error
) {}
