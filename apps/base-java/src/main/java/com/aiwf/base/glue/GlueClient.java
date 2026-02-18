package com.aiwf.base.glue;

import com.aiwf.base.config.AppProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Component
public class GlueClient {
    private final RestClient client;

    public GlueClient(AppProperties props) {
        this.client = RestClient.builder()
                .baseUrl(props.getGlueUrl())
                .build();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> runFlow(String jobId, String flow, Map<String, Object> payload) {
        // 约定 Python Glue 提供：POST /jobs/{jobId}/run/{flow}
        return client.post()
                .uri("/jobs/{jobId}/run/{flow}", jobId, flow)
                .contentType(MediaType.APPLICATION_JSON)
                .body(payload == null ? Map.of() : payload)
                .retrieve()
                .body(Map.class);
    }

    public Map<?, ?> health() {
        return client.get().uri("/health").retrieve().body(Map.class);
    }
}
