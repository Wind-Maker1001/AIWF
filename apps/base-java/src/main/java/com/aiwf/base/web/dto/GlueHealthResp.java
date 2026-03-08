package com.aiwf.base.web.dto;

import com.aiwf.base.glue.GlueHealthResult;

public record GlueHealthResp(boolean ok, GlueHealthResult glue) {
}
