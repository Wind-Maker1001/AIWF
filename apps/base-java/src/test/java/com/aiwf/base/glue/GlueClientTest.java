package com.aiwf.base.glue;

import com.aiwf.base.config.AppProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.client.MockRestServiceServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RestClientTest(GlueClient.class)
@EnableConfigurationProperties(AppProperties.class)
@TestPropertySource(properties = "aiwf.glue-url=http://glue.test")
class GlueClientTest {

    @Autowired
    private GlueClient glueClient;

    @Autowired
    private MockRestServiceServer server;

    @Test
    void runFlowPostsJsonBody() {
        server.expect(requestTo("http://glue.test/jobs/job-1/run/cleaning"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().json("""
                        {
                          "actor": "local",
                          "ruleset_version": "v1",
                          "params": {
                            "batch": 3
                          }
                        }
                        """))
                .andRespond(withSuccess("""
                        {
                          "ok": true,
                          "job_id": "job-1",
                          "flow": "cleaning",
                          "seconds": 1.25
                        }
                        """, MediaType.APPLICATION_JSON));

        GlueRunResult result = glueClient.runFlow(
                "job-1",
                "cleaning",
                new GlueRunRequest("local", "v1", java.util.Map.of("batch", 3))
        );

        assertThat(result.isOk()).isTrue();
        assertThat(result.getJobId()).isEqualTo("job-1");
        assertThat(result.getFlow()).isEqualTo("cleaning");
        assertThat(result.extras()).containsEntry("seconds", 1.25);
    }
}
