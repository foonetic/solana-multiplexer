/// Implements a simple Solana request multiplexer.
///
/// The multiplexer dispatches account subscriptions to multiple endpoints and
/// arbitrates responses to return the freshest data. WebSocket endpoints use
/// the streaming API. HTTP endpoints use the JSON RPC API, polling at the
/// specified frequency.
use clap::Parser;
use solana_multiplexer::{EndpointConfig, Server};
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

    #[clap(long, default_value = "0.0.0.0:8900")]
    listen_address: String,

    #[clap(long, default_value = "200")]
    poll_frequency_milliseconds: u64,
}

#[tokio::main(flavor = "current_thread")]
#[tracing::instrument]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let args = Args::parse();
    let endpoints: Vec<EndpointConfig> = args
        .endpoint
        .iter()
        .map(|s| {
            let url = if let Ok(url) = Url::parse(s) {
                url
            } else {
                panic!("unable to parse url: {}", s);
            };

            if s.starts_with("ws") {
                EndpointConfig::PubSub(url)
            } else if s.starts_with("http") {
                EndpointConfig::HTTP(
                    url,
                    std::time::Duration::from_millis(args.poll_frequency_milliseconds),
                )
            } else {
                panic!("endpoint could not be parsed as WebSocket or HTTP: {}", s);
            }
        })
        .collect();

    let mut server = Server::new();
    if let Err(err) = server.run(&endpoints, &args.listen_address).await {
        panic!("{}", err);
    }

    Ok(())
}
