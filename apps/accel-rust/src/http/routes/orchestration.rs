use accel_rust::app_state::AppState;
use axum::{
    Router,
    routing::{get, post},
};

pub(super) fn orchestration_routes() -> Router<AppState> {
    Router::new()
        .route("/workflow/run", post(crate::http::workflow_run_operator))
        .route("/tasks/{task_id}", get(crate::http::get_task_operator))
        .route(
            "/tasks/{task_id}/cancel",
            post(crate::http::cancel_task_operator),
        )
}
