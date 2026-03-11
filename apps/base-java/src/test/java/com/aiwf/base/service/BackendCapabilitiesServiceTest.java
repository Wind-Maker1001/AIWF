package com.aiwf.base.service;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.glue.GlueGateway;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BackendCapabilitiesServiceTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void snapshotAggregatesGlueAndAccelDomains() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/capabilities", exchange -> writeJson(exchange, 200, """
                {
                  "ok": true,
                  "domains": [
                    { "name": "governance", "operator_count": 2 }
                  ],
                  "workflow_domains": [
                    { "name": "transform", "operator_count": 3 }
                  ]
                }
                """));
        server.start();

        GlueGateway glue = mock(GlueGateway.class);
        when(glue.capabilities()).thenReturn(Map.of(
                "ok", true,
                "capabilities", Map.of(
                        "flow_domains", List.of(Map.of("name", "cleaning", "flow_names", List.of("cleaning")))
                )
        ));

        BackendCapabilitiesService service = new BackendCapabilitiesService(
                glue,
                props("http://127.0.0.1:" + server.getAddress().getPort()),
                RestClient.builder()
        );

        Map<String, Object> out = service.snapshot();

        assertThat(out).containsEntry("ok", true);
        assertThat(castMap(out.get("glue"))).containsEntry("url", "http://127.0.0.1:18081");
        assertThat(castMap(out.get("accel"))).containsEntry("url", "http://127.0.0.1:" + server.getAddress().getPort());
        assertThat(castMap(out.get("domains")).get("flow_domains")).asList().hasSize(1);
        assertThat(castMap(out.get("domains")).get("published_operator_domains")).asList().hasSize(1);
        assertThat(castMap(out.get("domains")).get("workflow_operator_domains")).asList().hasSize(1);
    }

    private AppProperties props(String accelUrl) {
        AppProperties props = new AppProperties();
        props.setRoot("D:\\AIWF");
        props.setBus("D:\\AIWF\\bus");
        props.setLake("D:\\AIWF\\lake");
        props.setGlueUrl("http://127.0.0.1:18081");
        props.setAccelUrl(accelUrl);
        props.setGlueConnectTimeoutMs(1000);
        props.setGlueReadTimeoutMs(1000);
        props.setAccelConnectTimeoutMs(1000);
        props.setAccelReadTimeoutMs(1000);
        return props;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private void writeJson(com.sun.net.httpserver.HttpExchange exchange, int status, String json) throws IOException {
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
