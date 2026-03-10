package com.aiwf.base.glue;

public interface GlueGateway {

    GlueHealthResult health();

    GlueRunResult runFlow(String jobId, String flow, GlueRunFlowReq request);
}
