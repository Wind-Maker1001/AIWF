package com.aiwf.base.web.dto;

public record OkResp(boolean ok) {
    public static OkResp success() {
        return new OkResp(true);
    }
}
