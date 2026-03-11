package com.aiwf.base.web;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.service.BackendCapabilitiesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BackendController.class)
@AutoConfigureMockMvc(addFilters = false)
class BackendControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private BackendCapabilitiesService capabilities;

    @MockBean
    private AppProperties appProperties;

    @Test
    void backendCapabilitiesExposeFlowAndOperatorDomains() throws Exception {
        when(capabilities.snapshot()).thenReturn(Map.of(
                "ok", true,
                "domains", Map.of(
                        "flow_domains", List.of(Map.of("name", "cleaning")),
                        "published_operator_domains", List.of(Map.of("name", "governance")),
                        "workflow_operator_domains", List.of(Map.of("name", "transform"))
                )
        ));

        mockMvc.perform(get("/api/v1/backend/capabilities"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.domains.flow_domains[0].name").value("cleaning"))
                .andExpect(jsonPath("$.domains.published_operator_domains[0].name").value("governance"))
                .andExpect(jsonPath("$.domains.workflow_operator_domains[0].name").value("transform"));
    }
}
