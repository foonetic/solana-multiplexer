use clap::Parser;
/// Implements a simple Solana request multiplexer.
///
/// The multiplexer dispatches account subscriptions to multiple endpoints and
/// arbitrates responses to return the freshest data. WebSocket endpoints use
/// the streaming API. HTTP endpoints use the JSON RPC API, polling at the
/// specified frequency.
use solana_multiplexer::{Endpoint, Multiplexer};
use tokio_tungstenite::tungstenite::Result;
use url::Url;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_values = &[
        "wss://api.mainnet-beta.solana.com:443",
        "https://api.mainnet-beta.solana.com:443",
        "wss://solana-api.projectserum.com:443",
        "https://solana-api.projectserum.com:443",
    ])]
    endpoint: Vec<String>,

    #[clap(long, default_value = "0.0.0:8900")]
    listen_address: String,

    #[clap(long, default_value = "100")]
    poll_frequency_milliseconds: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let endpoints: Vec<Endpoint> = args
        .endpoint
        .iter()
        .map(|s| {
            let url = if let Ok(url) = Url::parse(s) {
                url
            } else {
                panic!("unable to parse url: {}", s);
            };

            if s.starts_with("ws") {
                Endpoint::WebSocket(url)
            } else if s.starts_with("http") {
                Endpoint::Rpc(
                    url,
                    std::time::Duration::from_millis(args.poll_frequency_milliseconds),
                )
            } else {
                panic!("endpoint could not be parsed as WebSocket or HTTP: {}", s);
            }
        })
        .collect();

    let mut multiplexer = Multiplexer::new(&endpoints).await?;
    multiplexer.listen(&args.listen_address).await;

    Ok(())
}
