package com.aiwf.base.service;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.glue.GlueGateway;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.Map;

@Service
public class BackendCapabilitiesService {

    private final GlueGateway glue;
    private final RestClient accelClient;
    private final AppProperties props;

    public BackendCapabilitiesService(
            GlueGateway glue,
            AppProperties props,
            RestClient.Builder restClientBuilder
    ) {
        this.glue = glue;
        this.props = props;

        this.accelClient = restClientBuilder
                .requestFactory(BackendCapabilitiesSupport.createRequestFactory(
                        props.getAccelConnectTimeoutMs(),
                        props.getAccelReadTimeoutMs()
                ))
                .baseUrl(props.getAccelUrl())
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> snapshot() {
        Map<String, Object> glueCaps;
        try {
            glueCaps = glue.capabilities();
        } catch (RestClientException e) {
            glueCaps = BackendCapabilitiesSupport.unavailableSnapshot(
                    e.getMessage(),
                    "glue capabilities unavailable"
            );
        }

        Map<String, Object> accelCaps;
        try {
            accelCaps = accelClient.get()
                    .uri("/capabilities")
                    .retrieve()
                    .body(Map.class);
            if (accelCaps == null) {
                accelCaps = BackendCapabilitiesSupport.unavailableSnapshot(
                        "accel capabilities empty response",
                        "accel capabilities unavailable"
                );
            }
        } catch (RestClientException e) {
            accelCaps = BackendCapabilitiesSupport.unavailableSnapshot(
                    e.getMessage(),
                    "accel capabilities unavailable"
            );
        }

        return BackendCapabilitiesSupport.buildSnapshot(
                props.getGlueUrl(),
                glueCaps,
                props.getAccelUrl(),
                accelCaps
        );
    }
}
