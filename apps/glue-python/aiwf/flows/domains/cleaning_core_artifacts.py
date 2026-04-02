from __future__ import annotations

from aiwf.flows import cleaning_artifacts as registry


ARTIFACT_DOMAIN = {
    "name": "cleaning-core",
    "label": "Cleaning Core Artifacts",
    "backend": "python",
    "builtin": True,
}


def register_builtin_cleaning_artifacts(register_cleaning_artifact) -> None:
    register_cleaning_artifact(
        "csv_cleaned",
        artifact_id="csv_cleaned_001",
        kind="csv",
        path_key="cleaned_csv",
        sha_key="sha_csv",
        accel_output_key="cleaned_csv",
        local_path_resolver=registry._csv_local_path,
        local_writer=registry._write_csv_artifact,
        domain="cleaning-core",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_cleaning_artifact(
        "parquet_cleaned",
        artifact_id="parquet_cleaned_001",
        kind="parquet",
        path_key="cleaned_parquet",
        sha_key="sha_parquet",
        accel_output_key="cleaned_parquet",
        local_path_resolver=registry._parquet_local_path,
        local_writer=registry._write_parquet_artifact,
        required=True,
        domain="cleaning-core",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_cleaning_artifact(
        "profile_json",
        artifact_id="profile_json_001",
        kind="json",
        path_key="profile_json",
        sha_key="sha_profile",
        accel_output_key="profile_json",
        local_path_resolver=registry._profile_local_path,
        local_writer=registry._write_profile_artifact,
        domain="cleaning-core",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_cleaning_artifact(
        "quality_summary_json",
        artifact_id="quality_summary_json_001",
        kind="json",
        path_key="quality_summary_json",
        sha_key="sha_quality_summary_json",
        local_path_resolver=registry._quality_summary_local_path,
        local_writer=registry._write_quality_summary_artifact,
        domain="cleaning-core",
        domain_metadata=ARTIFACT_DOMAIN,
    )
    register_cleaning_artifact(
        "rejections_jsonl",
        artifact_id="rejections_jsonl_001",
        kind="jsonl",
        path_key="rejections_jsonl",
        sha_key="sha_rejections_jsonl",
        local_path_resolver=registry._rejections_local_path,
        local_writer=registry._write_rejections_artifact,
        domain="cleaning-core",
        domain_metadata=ARTIFACT_DOMAIN,
    )
