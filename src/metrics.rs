use hyper::{Body, Response};
use lazy_static::lazy_static;
use prometheus::{self, Encoder, Opts, TextEncoder};

lazy_static! {
    pub static ref METRICS_SERVE_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
        "metrics_serve_count",
        "number of times metrics have been served"
    )
    .unwrap();
    pub static ref CLIENT_REQUEST_COUNT: prometheus::IntCounter =
        prometheus::register_int_counter!(
            "client_request_count",
            "number of client requests received",
        )
        .unwrap();
    pub static ref PUBSUB_MESSAGE_COUNT: prometheus::IntCounterVec =
        prometheus::register_int_counter_vec!(
            Opts::new("pubsub_message_count", "PubSub message count"),
            &["endpoint", "type"],
        )
        .unwrap();
    pub static ref HTTP_MESSAGE_COUNT: prometheus::IntCounterVec =
        prometheus::register_int_counter_vec!(
            Opts::new("http_message_count", "HTTP message count"),
            &["endpoint", "response_code"],
        )
        .unwrap();
    pub static ref LATEST_SLOT_SEEN: prometheus::IntGaugeVec = prometheus::register_int_gauge_vec!(
        Opts::new(
            "latest_slot_seen",
            "Latest slot number seen across endpoints"
        ),
        &["endpoint"],
    )
    .unwrap();
}

pub fn serve_metrics() -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync + 'static>>
{
    METRICS_SERVE_COUNT.inc();

    let metric_families = prometheus::gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(Response::new(Body::from(String::from_utf8(buffer)?)))
}
