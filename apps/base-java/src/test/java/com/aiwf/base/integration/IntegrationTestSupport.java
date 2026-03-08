package com.aiwf.base.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
public abstract class IntegrationTestSupport {

    @Container
    @SuppressWarnings("resource")
    static final MSSQLServerContainer<?> SQL_SERVER = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2022-CU13-ubuntu-22.04")
            .acceptLicense()
            .withPassword("AiwfStrongPassw0rd!")
            .withInitScript("sql/integration-schema.sql")
            .withStartupTimeout(Duration.ofMinutes(5));

    static final Path BUS_ROOT = initBusRoot();
    static final GlueStubServer GLUE = startGlueStub();

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected ObjectMapper objectMapper;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", SQL_SERVER::getJdbcUrl);
        registry.add("spring.datasource.username", SQL_SERVER::getUsername);
        registry.add("spring.datasource.password", SQL_SERVER::getPassword);
        registry.add("aiwf.glueUrl", GLUE::baseUrl);
        registry.add("aiwf.bus", () -> BUS_ROOT.toString());
        registry.add("aiwf.apiKey", () -> "");
    }

    @BeforeEach
    void resetState() throws IOException {
        jdbc.update("DELETE FROM dbo.audit_log");
        jdbc.update("DELETE FROM dbo.artifacts");
        jdbc.update("DELETE FROM dbo.steps");
        jdbc.update("DELETE FROM dbo.workflow_tasks");
        jdbc.update("DELETE FROM dbo.jobs");
        GLUE.reset();
        Path jobsRoot = BUS_ROOT.resolve("jobs");
        if (Files.exists(jobsRoot)) {
            try (var paths = Files.walk(jobsRoot)) {
                paths.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .filter(path -> !path.equals(jobsRoot))
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
        Files.createDirectories(jobsRoot);
    }

    protected Map<String, Object> readJsonMap(String body) throws IOException {
        return objectMapper.readValue(body, new TypeReference<>() {});
    }

    protected String readJobStatus(String jobId) {
        return jdbc.queryForObject(
                "SELECT status FROM dbo.jobs WHERE job_id = ?",
                String.class,
                jobId
        );
    }

    protected int countRows(String table, String jobId) {
        return jdbc.queryForObject(
                "SELECT COUNT(1) FROM " + table + " WHERE job_id = ?",
                Integer.class,
                jobId
        );
    }

    protected static final class GlueStubServer {
        private final HttpServer server;
        private final ObjectMapper mapper = new ObjectMapper();
        private final List<CapturedRunRequest> runRequests = new ArrayList<>();
        private final Queue<StubResponse> runResponses = new ArrayDeque<>();

        private GlueStubServer(HttpServer server) {
            this.server = server;
            this.server.createContext("/health", this::handleHealth);
            this.server.createContext("/jobs", this::handleJobs);
            this.server.start();
        }

        String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        synchronized void reset() {
            runRequests.clear();
            runResponses.clear();
        }

        synchronized List<CapturedRunRequest> runRequests() {
            return List.copyOf(runRequests);
        }

        synchronized void enqueueRunResponse(int status, Map<String, Object> body) {
            runResponses.add(new StubResponse(status, body));
        }

        void stop() {
            server.stop(0);
        }

        private void handleHealth(HttpExchange exchange) throws IOException {
            writeJson(exchange, 200, Map.of("ok", true, "service", "glue-stub"));
        }

        private void handleJobs(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                writeJson(exchange, 405, Map.of("ok", false, "error", "method_not_allowed"));
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            if (parts.length < 6 || !"run".equals(parts[4])) {
                writeJson(exchange, 404, Map.of("ok", false, "error", "not_found"));
                return;
            }

            String jobId = parts[2];
            String flow = parts[5];
            Map<String, Object> payload = mapper.readValue(exchange.getRequestBody(), new TypeReference<>() {});
            StubResponse response;
            synchronized (this) {
                runRequests.add(new CapturedRunRequest(jobId, flow, payload));
                response = runResponses.poll();
            }

            if (response == null) {
                response = new StubResponse(200, Map.of("ok", true, "job_id", jobId, "flow", flow, "accepted", true));
            }
            writeJson(exchange, response.status(), response.body());
        }

        private void writeJson(HttpExchange exchange, int status, Map<String, Object> body) throws IOException {
            byte[] bytes = mapper.writeValueAsBytes(body);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }

    protected record CapturedRunRequest(String jobId, String flow, Map<String, Object> payload) {}

    protected record StubResponse(int status, Map<String, Object> body) {}

    private static Path initBusRoot() {
        try {
            return Files.createTempDirectory("aiwf-base-it-bus");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create integration bus root", e);
        }
    }

    private static GlueStubServer startGlueStub() {
        try {
            return new GlueStubServer(HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start glue stub", e);
        }
    }
}
