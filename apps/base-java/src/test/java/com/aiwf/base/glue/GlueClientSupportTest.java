package com.aiwf.base.glue;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.ResourceAccessException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GlueClientSupportTest {

    @Test
    void sanitizePositiveFallsBackForNonPositiveValues() {
        assertThat(GlueClientSupport.sanitizePositive(10, 3)).isEqualTo(10);
        assertThat(GlueClientSupport.sanitizePositive(0, 3)).isEqualTo(3);
        assertThat(GlueClientSupport.sanitizePositive(-1, 3)).isEqualTo(3);
    }

    @Test
    void safeMessageFallsBackToExceptionTypeWhenBlank() {
        assertThat(GlueClientSupport.safeMessage(new RuntimeException("boom"))).isEqualTo("boom");
        assertThat(GlueClientSupport.safeMessage(new RuntimeException(" "))).isEqualTo("RuntimeException");
    }

    @Test
    void executeWithRetryRetriesUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();

        String result = GlueClientSupport.executeWithRetry(
                LoggerFactory.getLogger(GlueClientSupportTest.class),
                "GET",
                "/health",
                2,
                0L,
                () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new ResourceAccessException("warming_up");
                    }
                    return "ok";
                }
        );

        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void executeWithRetryThrowsLastErrorAfterExhaustion() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> GlueClientSupport.executeWithRetry(
                LoggerFactory.getLogger(GlueClientSupportTest.class),
                "GET",
                "/health",
                2,
                0L,
                () -> {
                    attempts.incrementAndGet();
                    throw new ResourceAccessException("down");
                }
        ))
                .isInstanceOf(ResourceAccessException.class)
                .hasMessageContaining("down");

        assertThat(attempts.get()).isEqualTo(2);
    }
}
