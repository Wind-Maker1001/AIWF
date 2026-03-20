use crate::{
    api_types::{AggregatePushdownReq, EntityExtractReq, NormalizeSchemaReq, RulesPackageGetReq, RulesPackagePublishReq},
    misc_ops::{
        run_aggregate_pushdown_v1, run_entity_extract_v1, run_normalize_schema_v1,
        run_rules_package_get_v1, run_rules_package_publish_v1,
    },
};
use axum::{Json, response::IntoResponse};

use super::support;

pub(crate) async fn rules_package_publish_v1_operator(
    Json(req): Json<RulesPackagePublishReq>,
) -> impl IntoResponse {
    match run_rules_package_publish_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("rules_package_publish_v1", error),
    }
}

pub(crate) async fn rules_package_get_v1_operator(
    Json(req): Json<RulesPackageGetReq>,
) -> impl IntoResponse {
    match run_rules_package_get_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("rules_package_get_v1", error),
    }
}

pub(crate) async fn normalize_schema_v1_operator(
    Json(req): Json<NormalizeSchemaReq>,
) -> impl IntoResponse {
    match run_normalize_schema_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("normalize_schema_v1", error),
    }
}

pub(crate) async fn entity_extract_v1_operator(
    Json(req): Json<EntityExtractReq>,
) -> impl IntoResponse {
    match run_entity_extract_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("entity_extract_v1", error),
    }
}

pub(crate) async fn aggregate_pushdown_v1_operator(
    Json(req): Json<AggregatePushdownReq>,
) -> impl IntoResponse {
    match run_aggregate_pushdown_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("aggregate_pushdown_v1", error),
    }
}
