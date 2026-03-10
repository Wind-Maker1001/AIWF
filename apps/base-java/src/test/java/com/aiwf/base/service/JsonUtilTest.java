package com.aiwf.base.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonUtilTest {

    @Test
    void toJsonReturnsFallbackOnSerializationFailure() {
        String json = JsonUtil.toJson(new BrokenBean());

        assertThat(json).isEqualTo("{}");
    }

    @Test
    void toJsonOrNullReturnsStringUnchanged() {
        assertThat(JsonUtil.toJsonOrNull("{\"ok\":true}")).isEqualTo("{\"ok\":true}");
    }

    static final class BrokenBean {
        public String getValue() {
            throw new IllegalStateException("broken");
        }
    }
}
