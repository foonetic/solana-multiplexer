/// Implements a simple Solana request multiplexer.
///
/// The multiplexer dispatches account subscriptions to multiple validators and
/// arbitrates responses to return the freshest data.
use solana_multiplexer::{Endpoint, Multiplexer};
use tokio_tungstenite::tungstenite::Result;
use url::Url;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = vec![
        Endpoint {
            websocket: Some(Url::parse("wss://api.mainnet-beta.solana.com:443").unwrap()),
            rpc: Some(Url::parse("https://api.mainnet-beta.solana.com:443").unwrap()),
        },
        Endpoint {
            websocket: Some(Url::parse("wss://solana-api.projectserum.com:443").unwrap()),
            rpc: Some(Url::parse("https://solana-api.projectserum.com:443").unwrap()),
        },
    ];
    let mut multiplexer = Multiplexer::new(&urls).await.unwrap();
    multiplexer.listen("0.0.0.0:8900").await;

    Ok(())
}
