/// Demo of a client interacting with the multiplexer.
///
/// Example invocation:
///
///     cargo run
///         --endpoint https://api.devnet.solana.com:443
///         --endpoint wss://api.devnet.solana.com:443
///
///     cargo run --example multiplexer_client
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::info;
use url::Url;

#[derive(Parser, Debug)]
struct Args {
    /// The multiplexer endpoint. Note that this can also be a standard JSON RPC
    /// endpoint since the multiplexer has the same API.
    #[clap(long, default_value = "0.0.0.0:8900")]
    address: String,
}

#[tokio::main(flavor = "current_thread")]
#[tracing::instrument]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let args = Args::parse();

    // Example of a standard HTTP request.
    let client = reqwest::Client::new();
    let response = client
        .post(Url::parse(&format!("http://{}", args.address))?)
        .body(
            r#"
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getHealth"
            }
            "#,
        )
        .header("content-type", "application/json")
        .send()
        .await?;
    let text = response.text().await?;
    info!("http response: {}", text);

    // Example of a websocket subscription.
    let (mut ws, ws_response) =
        tokio_tungstenite::connect_async(Url::parse(&format!("ws://{}", args.address))?).await?;
    info!("pubsub upgrade response: {:?}", ws_response);
    ws.send(Message::Text(
        r#"
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "slotSubscribe"
        }
        "#
        .to_string(),
    ))
    .await?;

    ws.send(Message::Text(
        r#"
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "rootSubscribe"
        }
        "#
        .to_string(),
    ))
    .await?;

    while let Some(message) = ws.next().await {
        info!("pubsub message: {:?}", message);
    }

    Ok(())
}
