package com.aiwf.base.glue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GlueRunFlowContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void requestSerializationMatchesFixture() throws Exception {
        GlueJobContext jobContext = new GlueJobContext(
                "D:\\AIWF\\bus\\jobs\\job-123",
                "D:\\AIWF\\bus\\jobs\\job-123\\stage",
                "D:\\AIWF\\bus\\jobs\\job-123\\artifacts",
                "D:\\AIWF\\bus\\jobs\\job-123\\evidence"
        );
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("office_lang", "zh");
        params.put("job_root", jobContext.jobRoot());

        GlueRunFlowReq req = new GlueRunFlowReq(
                "job-123",
                "cleaning",
                "local",
                "v1",
                "trace-123",
                jobContext,
                params
        );

        JsonNode actual = objectMapper.valueToTree(req);
        JsonNode expected = readJsonNode("contracts/glue-run-flow-request-minimal.json");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void responseFixtureParsesIntoRunResult() throws Exception {
        Map<String, Object> body = readJsonMap("contracts/glue-run-flow-response-sample.json");

        GlueRunFlowResp resp = GlueRunFlowResp.fromMap(body, "job-123", "cleaning");
        GlueRunResult result = resp.toRunResult();

        assertThat(resp.ok()).isTrue();
        assertThat(resp.jobId()).isEqualTo("job-123");
        assertThat(resp.flow()).isEqualTo("cleaning");
        assertThat(resp.outputHash()).isEqualTo("abc123");
        assertThat(resp.errorId()).isEqualTo("trace-123");

        assertThat(result.isOk()).isTrue();
        assertThat(result.getJobId()).isEqualTo("job-123");
        assertThat(result.getFlow()).isEqualTo("cleaning");
        assertThat(result.extras()).containsEntry("output_hash", "abc123");
        assertThat(result.extras()).containsKey("artifacts");
        assertThat(result.extras()).containsKey("profile");
        assertThat(result.extras()).containsKey("accel");
        assertThat(result.extras()).containsEntry("error_id", "trace-123");
    }

    private JsonNode readJsonNode(String resourcePath) throws Exception {
        try (InputStream in = resource(resourcePath)) {
            return objectMapper.readTree(in);
        }
    }

    private Map<String, Object> readJsonMap(String resourcePath) throws Exception {
        try (InputStream in = resource(resourcePath)) {
            return objectMapper.readValue(in, new TypeReference<>() {});
        }
    }

    private InputStream resource(String resourcePath) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        if (in == null) {
            throw new IllegalStateException("missing resource: " + resourcePath);
        }
        return in;
    }
}
