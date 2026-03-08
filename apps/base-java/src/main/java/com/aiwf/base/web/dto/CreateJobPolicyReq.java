package com.aiwf.base.web.dto;

import java.util.Map;

public final class CreateJobPolicyReq extends FlexibleBody {

    public Map<String, Object> policy() {
        return extras();
    }
}
