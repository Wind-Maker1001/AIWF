package com.aiwf.base.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class AppPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class,
                    ValidationAutoConfiguration.class
            ))
            .withUserConfiguration(ConfigEnable.class);

    @Test
    void bindsValidProperties() {
        contextRunner.withPropertyValues(
                        "aiwf.root=.",
                        "aiwf.bus=.\\bus",
                        "aiwf.lake=.\\lake",
                        "aiwf.glue-url=http://127.0.0.1:18081",
                        "aiwf.glue-connect-timeout-ms=1500",
                        "aiwf.glue-read-timeout-ms=2500",
                        "aiwf.glue-run-max-attempts=2",
                        "aiwf.glue-health-max-attempts=4",
                        "aiwf.glue-retry-delay-ms=10"
                )
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    AppProperties props = context.getBean(AppProperties.class);
                    assertThat(props.getRoot()).isEqualTo(".");
                    assertThat(props.getBus()).isEqualTo(".\\bus");
                    assertThat(props.getLake()).isEqualTo(".\\lake");
                    assertThat(props.getGlueUrl()).isEqualTo("http://127.0.0.1:18081");
                    assertThat(props.getGlueConnectTimeoutMs()).isEqualTo(1500);
                    assertThat(props.getGlueReadTimeoutMs()).isEqualTo(2500);
                    assertThat(props.getGlueRunMaxAttempts()).isEqualTo(2);
                    assertThat(props.getGlueHealthMaxAttempts()).isEqualTo(4);
                    assertThat(props.getGlueRetryDelayMs()).isEqualTo(10L);
                });
    }

    @Test
    void rejectsNonPositiveTimeouts() {
        contextRunner.withPropertyValues(
                        "aiwf.root=.",
                        "aiwf.bus=.\\bus",
                        "aiwf.lake=.\\lake",
                        "aiwf.glue-url=http://127.0.0.1:18081",
                        "aiwf.glue-connect-timeout-ms=0"
                )
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("glueConnectTimeoutMs");
                });
    }

    @Test
    void rejectsBlankGlueUrl() {
        contextRunner.withPropertyValues(
                        "aiwf.root=.",
                        "aiwf.bus=.\\bus",
                        "aiwf.lake=.\\lake",
                        "aiwf.glue-url= "
                )
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("glueUrl");
                });
    }
}
