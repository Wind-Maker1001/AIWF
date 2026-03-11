package com.aiwf.base.glue;

public interface GlueGateway {

    GlueHealthResult health();

    java.util.Map<String, Object> capabilities();

    GlueRunResult runFlow(String jobId, String flow, GlueRunFlowReq request);
}
