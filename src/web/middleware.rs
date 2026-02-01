//! API middleware

use axum::{
    body::Body,
    http::{Request, Response},
    middleware::Next,
};
use std::time::Instant;
use tracing::info;

/// Request logging middleware
pub async fn logging_middleware(
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let start = Instant::now();
    
    let response = next.run(request).await;
    
    let elapsed = start.elapsed();
    let status = response.status();
    
    info!(
        "{} {} - {} ({:?})",
        method,
        uri,
        status.as_u16(),
        elapsed
    );
    
    response
}
