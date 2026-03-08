package com.aiwf.base.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@ConfigurationProperties(prefix = "aiwf")
public class AppProperties {
    private String root;
    private String bus;
    private String lake;
    private String glueUrl;
    private String apiKey;
    private int glueConnectTimeoutMs = 3000;
    private int glueReadTimeoutMs = 30000;
    private int glueRunMaxAttempts = 1;
    private int glueHealthMaxAttempts = 3;
    private long glueRetryDelayMs = 250L;

    public String getRoot() { return root; }
    public void setRoot(String root) { this.root = root; }

    public String getBus() { return bus; }
    public void setBus(String bus) { this.bus = bus; }

    public String getLake() { return lake; }
    public void setLake(String lake) { this.lake = lake; }

    public String getGlueUrl() { return glueUrl; }
    public void setGlueUrl(String glueUrl) { this.glueUrl = glueUrl; }

    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    public int getGlueConnectTimeoutMs() { return glueConnectTimeoutMs; }
    public void setGlueConnectTimeoutMs(int glueConnectTimeoutMs) { this.glueConnectTimeoutMs = glueConnectTimeoutMs; }

    public int getGlueReadTimeoutMs() { return glueReadTimeoutMs; }
    public void setGlueReadTimeoutMs(int glueReadTimeoutMs) { this.glueReadTimeoutMs = glueReadTimeoutMs; }

    public int getGlueRunMaxAttempts() { return glueRunMaxAttempts; }
    public void setGlueRunMaxAttempts(int glueRunMaxAttempts) { this.glueRunMaxAttempts = glueRunMaxAttempts; }

    public int getGlueHealthMaxAttempts() { return glueHealthMaxAttempts; }
    public void setGlueHealthMaxAttempts(int glueHealthMaxAttempts) { this.glueHealthMaxAttempts = glueHealthMaxAttempts; }

    public long getGlueRetryDelayMs() { return glueRetryDelayMs; }
    public void setGlueRetryDelayMs(long glueRetryDelayMs) { this.glueRetryDelayMs = glueRetryDelayMs; }
}
