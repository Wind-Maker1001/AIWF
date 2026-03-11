use accel_rust::app_state::AppState;
use axum::Router;

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
