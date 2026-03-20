use crate::transform_support::operator_allowed_for_tenant;
use accel_rust::app_state::AppState;
use axum::{
    Router,
    body::Body,
    extract::Request,
    middleware::{self, Next},
    response::Response,
};

#[path = "routes_support.rs"]
mod support;

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
    let Some(operator) = support::operator_name_from_path(&path) else {
        return next.run(request).await;
    };

    let (parts, body) = request.into_parts();
    let bytes = match support::request_body_bytes(&parts.method, body).await {
        Ok(bytes) => bytes,
        Err(err) => return support::bad_request_response(&operator, &err),
    };
    let tenant_id = support::tenant_from_query(parts.uri.query())
        .or_else(|| support::tenant_from_body_bytes(&bytes));

    if !operator_allowed_for_tenant(&operator, tenant_id.as_deref()) {
        return support::forbidden_response(&operator, tenant_id.as_deref());
    }

    let request = Request::from_parts(parts, Body::from(bytes));
    next.run(request).await
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
