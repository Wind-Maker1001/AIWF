use crate::transform_support::operator_allowed_for_tenant;
use accel_rust::app_state::AppState;
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::{Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use url::form_urlencoded;

#[path = "routes/system.rs"]
mod system;
#[path = "routes/transform.rs"]
mod transform;
#[path = "routes/table.rs"]
mod table;
#[path = "routes/integration.rs"]
mod integration;
#[path = "routes/storage_schema.rs"]
mod storage_schema;
#[path = "routes/analysis.rs"]
mod analysis;
#[path = "routes/governance.rs"]
mod governance;
#[path = "routes/orchestration.rs"]
mod orchestration;

pub(super) fn operator_guarded(router: Router<AppState>) -> Router<AppState> {
    router.route_layer(middleware::from_fn(enforce_operator_route_policy))
}

async fn enforce_operator_route_policy(request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    let Some(operator) = operator_name_from_path(&path) else {
        return next.run(request).await;
    };

    let (parts, body) = request.into_parts();
    let bytes = match request_body_bytes(&parts.method, body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "ok": false,
                    "operator": operator,
                    "status": "failed",
                    "error": err,
                })),
            )
                .into_response();
        }
    };
    let tenant_id = tenant_from_query(parts.uri.query()).or_else(|| tenant_from_body_bytes(&bytes));

    if !operator_allowed_for_tenant(&operator, tenant_id.as_deref()) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "ok": false,
                "operator": operator,
                "status": "failed",
                "error": format!(
                    "operator_forbidden: tenant={} operator={}",
                    tenant_id.as_deref().unwrap_or("default"),
                    operator
                ),
            })),
        )
            .into_response();
    }

    let request = Request::from_parts(parts, Body::from(bytes));
    next.run(request).await
}

fn tenant_from_query(query: Option<&str>) -> Option<String> {
    query.and_then(|raw| {
        form_urlencoded::parse(raw.as_bytes())
            .find_map(|(key, value)| {
                if key == "tenant_id" {
                    let tenant = value.trim().to_string();
                    if tenant.is_empty() { None } else { Some(tenant) }
                } else {
                    None
                }
            })
    })
}

fn operator_name_from_path(path: &str) -> Option<String> {
    let suffix = path.strip_prefix("/operators/")?.trim_matches('/');
    if suffix.is_empty() {
        return None;
    }
    if let Some(name) = suffix.strip_prefix("transform_rows_v2/") {
        if matches!(name, "submit" | "stream" | "cache_stats" | "cache_clear") {
            return Some("transform_rows_v2".to_string());
        }
    }
    if let Some(action) = suffix.strip_prefix("rules_package_v1/") {
        let action = action.trim_matches('/');
        if !action.is_empty() {
            return Some(format!("rules_package_{action}_v1"));
        }
    }
    let normalized = suffix.replace('/', "_");
    if let Some(base) = normalized.strip_suffix("_apply") {
        return Some(base.to_string());
    }
    if let Some(base) = normalized.strip_suffix("_validate") {
        return Some(base.to_string());
    }
    Some(normalized)
}

fn tenant_from_value(value: &Value) -> Option<String> {
    value
        .as_object()
        .and_then(|obj| obj.get("tenant_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|tenant| !tenant.is_empty())
        .map(ToOwned::to_owned)
}

async fn request_body_bytes(method: &Method, body: Body) -> Result<Bytes, String> {
    if *method == Method::GET || *method == Method::HEAD {
        return Ok(Bytes::new());
    }
    to_bytes(body, 64 * 1024 * 1024)
        .await
        .map_err(|err| format!("failed to inspect operator request body: {err}"))
}

fn tenant_from_body_bytes(bytes: &Bytes) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|parsed| tenant_from_value(&parsed))
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(system::system_routes())
        .merge(transform::transform_routes())
        .merge(table::table_routes())
        .merge(integration::integration_routes())
        .merge(storage_schema::storage_schema_routes())
        .merge(analysis::analysis_routes())
        .merge(governance::governance_routes())
        .merge(orchestration::orchestration_routes())
        .with_state(state)
}
