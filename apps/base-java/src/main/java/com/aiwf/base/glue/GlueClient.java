package com.aiwf.base.glue;

import com.aiwf.base.config.AppProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Component
public class GlueClient implements GlueGateway {
    private final RestClient client;

    public GlueClient(AppProperties props, RestClient.Builder restClientBuilder) {
        this.client = restClientBuilder
                .baseUrl(props.getGlueUrl())
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public GlueRunResult runFlow(String jobId, String flow, GlueRunRequest request) {
        Map<String, Object> response = client.post()
                .uri("/jobs/{jobId}/run/{flow}", jobId, flow)
                .contentType(MediaType.APPLICATION_JSON)
                .body(request == null ? Map.of() : request.toPayload())
                .retrieve()
                .body(Map.class);
        return GlueRunResult.fromMap(response, jobId, flow);
    }

    @Override
    @SuppressWarnings("unchecked")
    public GlueHealthResult health() {
        Map<String, Object> response = client.get().uri("/health").retrieve().body(Map.class);
        return GlueHealthResult.fromMap(response);
    }
}
