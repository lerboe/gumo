//! A minimal HTTP echo server, used as a benchmark target for gumo.

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use tokio::net::TcpListener;

/// The echo service.
///
/// - `GET /health` answers `200 OK` with an empty body.
/// - `/echo` (any method) answers with the request body and its `Content-Type`.
pub fn app() -> Router {
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/echo", any(echo))
}

async fn echo(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    let mut response = body.into_response();
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type.clone());
    }
    response
}

/// Serves the echo service on `listener`, accepting HTTP/1.1 and HTTP/2 (prior knowledge).
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    axum::serve(listener, app()).await
}
