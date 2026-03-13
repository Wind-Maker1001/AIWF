package com.aiwf.base.glue;

import com.aiwf.base.config.AppProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GlueClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void runFlowPostsJsonBody() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<Map<String, Object>> body = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/jobs/job-1/run/cleaning", exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().getPath());
            body.set(readJsonMap(exchange));
            writeJson(exchange, 200, """
                    {
                      "ok": true,
                      "job_id": "job-1",
                      "flow": "cleaning",
                      "seconds": 1.25
                    }
                    """);
        });
        server.start();

        GlueClient glueClient = new GlueClient(appProperties(serverBaseUrl()), RestClient.builder());
        GlueRunResult result = glueClient.runFlow("job-1", "cleaning", new GlueRunFlowReq(
                "job-1",
                "cleaning",
                "local",
                "v1",
                "trace-1",
                new GlueJobContext("D:\\AIWF\\bus\\jobs\\job-1", "D:\\AIWF\\bus\\jobs\\job-1\\stage", "D:\\AIWF\\bus\\jobs\\job-1\\artifacts", "D:\\AIWF\\bus\\jobs\\job-1\\evidence"),
                Map.of("batch", 3)
        ));

        assertThat(result.isOk()).isTrue();
        assertThat(result.getJobId()).isEqualTo("job-1");
        assertThat(result.getFlow()).isEqualTo("cleaning");
        assertThat(result.extras()).containsEntry("seconds", 1.25);
        assertThat(method.get()).isEqualTo("POST");
        assertThat(path.get()).isEqualTo("/jobs/job-1/run/cleaning");
        assertThat(body.get()).containsEntry("job_id", "job-1");
        assertThat(body.get()).containsEntry("flow", "cleaning");
        assertThat(body.get()).containsEntry("actor", "local");
        assertThat(body.get()).containsEntry("ruleset_version", "v1");
        assertThat(body.get()).containsEntry("trace_id", "trace-1");
        assertThat(body.get()).containsKey("job_context");
        assertThat(castMap(body.get().get("job_context")))
                .containsEntry("job_root", "D:\\AIWF\\bus\\jobs\\job-1")
                .containsEntry("stage_dir", "D:\\AIWF\\bus\\jobs\\job-1\\stage")
                .containsEntry("artifacts_dir", "D:\\AIWF\\bus\\jobs\\job-1\\artifacts")
                .containsEntry("evidence_dir", "D:\\AIWF\\bus\\jobs\\job-1\\evidence");
        assertThat(body.get()).containsKey("params");
        assertThat(castMap(body.get().get("params"))).containsEntry("batch", 3);
    }

    @Test
    void healthRetriesBeforeSucceeding() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/health", exchange -> {
            int current = attempts.incrementAndGet();
            if (current == 1) {
                writeJson(exchange, 503, """
                        {
                          "ok": false,
                          "error": "warming_up"
                        }
                        """);
                return;
            }
            writeJson(exchange, 200, """
                    {
                      "ok": true,
                      "service": "glue-python"
                    }
                    """);
        });
        server.start();

        AppProperties props = appProperties(serverBaseUrl());
        props.setGlueHealthMaxAttempts(2);
        props.setGlueRetryDelayMs(1);
        GlueClient glueClient = new GlueClient(props, RestClient.builder());

        GlueHealthResult result = glueClient.health();

        assertThat(result.isOk()).isTrue();
        assertThat(result.extras()).containsEntry("service", "glue-python");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void runFlowDoesNotRetryWhenEnabled() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/jobs/job-2/run/cleaning", exchange -> {
            attempts.incrementAndGet();
            writeJson(exchange, 502, """
                    {
                      "ok": false,
                      "error": "temporary_gateway"
                    }
                    """);
        });
        server.start();

        AppProperties props = appProperties(serverBaseUrl());
        props.setGlueRunMaxAttempts(2);
        props.setGlueRetryDelayMs(1);
        GlueClient glueClient = new GlueClient(props, RestClient.builder());

        assertThatThrownBy(() -> glueClient.runFlow("job-2", "cleaning", new GlueRunFlowReq(
                "job-2",
                "cleaning",
                "ops",
                "v2",
                "trace-2",
                new GlueJobContext("D:\\AIWF\\bus\\jobs\\job-2", "D:\\AIWF\\bus\\jobs\\job-2\\stage", "D:\\AIWF\\bus\\jobs\\job-2\\artifacts", "D:\\AIWF\\bus\\jobs\\job-2\\evidence"),
                Map.of()
        )))
                .isInstanceOf(RestClientException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    private AppProperties appProperties(String baseUrl) {
        AppProperties props = new AppProperties();
        props.setGlueUrl(baseUrl);
        props.setGlueConnectTimeoutMs(1000);
        props.setGlueReadTimeoutMs(2000);
        props.setGlueRunMaxAttempts(1);
        props.setGlueHealthMaxAttempts(1);
        props.setGlueRetryDelayMs(1);
        return props;
    }

    private String serverBaseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private Map<String, Object> readJsonMap(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        if (bytes.length == 0) {
            return Map.of();
        }
        return objectMapper.readValue(bytes, Map.class);
    }

    private void writeJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        } finally {
            exchange.close();
        }
    }
}
