use crate::option::CONFIG;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use lazy_static::lazy_static;
use std::cmp::min;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

// Create a global variable to store signal status
lazy_static! {
    static ref SIGNAL_RECEIVED: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

pub async fn liveness() -> HttpResponse {
    HttpResponse::new(StatusCode::OK)
}

// Initialize the signal handler and handle signals
pub async fn handle_signals() {
    let signal_received = SIGNAL_RECEIVED.clone();

    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to set up SIGTERM signal handler");
        eprintln!("Signal handler task started");
        loop {
            match sigterm.recv().await {
                Some(_) => {
                    eprintln!("Received SIGTERM signal");
                    let mut shutdown_flag = signal_received.lock().await;
                    *shutdown_flag = true;
                    eprintln!("Current signal flag value: {:?}", *shutdown_flag);


                }
                None => {
                    eprintln!("Signal handler received None, indicating an error or end of stream");
                }
            }
        };
}

pub async fn readiness() -> HttpResponse {
    // Check if the application has received a shutdown signal
    let shutdown_flag = SIGNAL_RECEIVED.lock().await;
    if *shutdown_flag {
        return HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE);
    }

    // Check the object store connection
    if CONFIG.storage().get_object_store().check().await.is_ok() {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
    }
}
