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
}
