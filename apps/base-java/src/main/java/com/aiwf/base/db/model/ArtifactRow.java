package com.aiwf.base.db.model;

public record ArtifactRow(
        String artifactId,
        String kind,
        String path,
        String sha256,
        Object createdAt
) {
}
