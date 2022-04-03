use crate::messages::*;
use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info};
use url::Url;

/// Represents a Solana RPC endpoint.
pub enum EndpointConfig {
    /// Represents a standard HTTP endpoint. The endpoint must be served at the
    /// given Url. The endpoint will be polled at the given frequency.
    HTTP(Url, Duration),

    /// Represents a standard WebSocket PubSub endpoint.
    PubSub(Url),
}

/// Internal representation of the endpoint.
enum EndpointProtocol {
    HTTP(Arc<reqwest::Client>, Url, Duration),
    PubSub(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

/// Receives instructions from the server for a single endpoint. Sends data back
/// to the server to be arbitrated.
pub struct EndpointManager {
    send: UnboundedSender<EndpointToServer>,
    receive: UnboundedReceiver<ServerToEndpoint>,
}

/// Spawns processes to listen to the input endpoints. Returns a channel that
/// the server can use to listen to all endpoint activity, and a vector of
/// channels that can be used to communicate to all endpoints. Each endpoint is
/// responsible for ensuring that the returned subscription id echoes the input
/// id sent by the server.
pub async fn spawn(
    config: &[EndpointConfig],
) -> (
    UnboundedReceiver<EndpointToServer>,
    Vec<UnboundedSender<ServerToEndpoint>>,
) {
    let (endpoint_to_server, receive_from_endpoints) = unbounded_channel();
    let http_client = Arc::new(reqwest::Client::new());
    let mut endpoints = Vec::new();
    for endpoint in config.iter() {
        let (server_to_endpoint, receive_from_server) = unbounded_channel();
        let mut endpoint_manager = EndpointManager {
            send: endpoint_to_server.clone(),
            receive: receive_from_server,
        };

        let protocol = match endpoint {
            EndpointConfig::HTTP(url, frequency) => {
                info!(new_http_endpoint = url.as_str());
                EndpointProtocol::HTTP(http_client.clone(), url.clone(), frequency.clone())
            }

            EndpointConfig::PubSub(url) => {
                info!(new_pubsub_endpoint = url.as_str());
                let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
                EndpointProtocol::PubSub(ws)
            }
        };

        tokio::spawn(async move {
            endpoint_manager.run(protocol).await;
        });

        endpoints.push(server_to_endpoint);
    }
    (receive_from_endpoints, endpoints)
}

impl EndpointManager {
    /// Manages either an HTTP or PubSub endpoint.
    async fn run(&mut self, protocol: EndpointProtocol) {
        match protocol {
            EndpointProtocol::HTTP(client, url, frequency) => {
                self.run_http(client, url, frequency).await;
            }
            EndpointProtocol::PubSub(websocket) => {
                self.run_pubsub(websocket).await;
            }
        };
    }

    /// Manages an HTTP endpoint.
    async fn run_http(&mut self, client: Arc<reqwest::Client>, url: Url, frequency: Duration) {
        let mut unsubscribe_by_id = HashMap::new();
        while let Some(from_server) = self.receive.recv().await {
            match from_server {
                ServerToEndpoint::Instruction(instruction) => match instruction.method {
                    Method::accountSubscribe => {
                        let unsubscribe = Arc::new(Mutex::new(false));
                        unsubscribe_by_id.insert(instruction.id, unsubscribe.clone());
                        let mut instruction = instruction.clone();
                        instruction.method = Method::getAccountInfo;
                        info!(new_http_subscription_id = instruction.id);
                        let instruction = serde_json::to_string(&instruction).unwrap();
                        let mut endpoint = PollHTTP {
                            client: client.clone(),
                            url: url.clone(),
                            frequency: frequency.clone(),
                            instruction: instruction,
                            unsubscribe: unsubscribe.clone(),
                            send: self.send.clone(),
                        };
                        tokio::spawn(async move {
                            endpoint.run().await;
                        });
                    }
                    _ => {}
                },
            }
        }
    }

    /// Manages a PubSub endpoint.
    async fn run_pubsub(&mut self, websocket: WebSocketStream<MaybeTlsStream<TcpStream>>) {
        let (mut to_endpoint, from_endpoint) = websocket.split();
        let id_to_subscription = Arc::new(Mutex::new(HashMap::new()));

        let mut pubsub = PollPubSub {
            from_endpoint,
            subscription_to_id: HashMap::new(),
            send: self.send.clone(),
            id_to_subscription: id_to_subscription.clone(),
        };
        tokio::spawn(async move {
            pubsub.run().await;
        });

        while let Some(from_server) = self.receive.recv().await {
            match from_server {
                ServerToEndpoint::Instruction(instruction) => match instruction.method {
                    Method::accountSubscribe => {
                        let instruction = serde_json::to_string(&instruction).unwrap();
                        if let Err(err) = to_endpoint.send(Message::from(instruction)).await {
                            error!(
                                instruction_to_endpoint_channel_failure = err.to_string().as_str()
                            );
                        }
                    }
                    _ => {}
                },
            }
        }
    }
}

/// Polls messages from the PubSub endpoint and passes them to the server as
/// needed. Subscription replies do not need to be passes back to the server
/// since the endpoint is responsible for managing those subscriptions. The
/// server will always reply with the logical global subscription id rather than
/// each endpoint's specific subscription id.
struct PollPubSub {
    from_endpoint: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    subscription_to_id: HashMap<u64, u64>,
    send: UnboundedSender<EndpointToServer>,
    id_to_subscription: Arc<Mutex<HashMap<u64, u64>>>,
}

impl PollPubSub {
    async fn run(&mut self) {
        while let Some(from_endpoint) = self.from_endpoint.next().await {
            let data = from_endpoint;
            if data.is_err() {
                error!(pubsub_read_failure = data.unwrap_err().to_string().as_str());
                continue;
            }
            let data = data.unwrap().into_text();
            if data.is_err() {
                error!(pubsub_read_failure = data.unwrap_err().to_string().as_str());
                continue;
            }
            let data = data.unwrap();

            if let Ok(mut account_notification) = serde_json::from_str::<AccountNotification>(&data)
            {
                let id = {
                    self.subscription_to_id
                        .get(&account_notification.params.subscription)
                        .map(|v| *v)
                };

                if let Some(id) = id {
                    account_notification.params.subscription = id;
                    if let Err(err) = self
                        .send
                        .send(EndpointToServer::AccountNotification(account_notification))
                    {
                        error!(
                            pubsub_to_server_channel_failure = err.to_string().as_str(),
                            subscription_id = id
                        );
                    }
                }
            } else if let Ok(subscription_reply) = serde_json::from_str::<SubscriptionReply>(&data)
            {
                // Map to and from the subscription id returned by the
                // endpoint. This will be needed to translate replies into
                // the global id, and also to unsubscribe.
                {
                    let mut id_to_subscription = self.id_to_subscription.lock().unwrap();
                    id_to_subscription.insert(subscription_reply.id, subscription_reply.result);
                }
                self.subscription_to_id
                    .insert(subscription_reply.result, subscription_reply.id);
                info!(
                    new_pubsub_global_id = subscription_reply.id,
                    new_pubsub_subscription_id = subscription_reply.result
                );
            }
        }
    }
}

/// Regularly polls an HTTP endpoint at some configured frequency.
struct PollHTTP {
    client: Arc<reqwest::Client>,
    url: Url,
    frequency: Duration,
    instruction: String,
    unsubscribe: Arc<Mutex<bool>>,
    send: UnboundedSender<EndpointToServer>,
}

impl PollHTTP {
    async fn run(&mut self) {
        let mut interval = time::interval(self.frequency);
        loop {
            // If we are unsubscribed, quit.
            {
                if *self.unsubscribe.lock().unwrap() {
                    return;
                }
            }

            if let Ok(result) = self
                .client
                .post(self.url.clone())
                .body(self.instruction.clone())
                .header("content-type", "application/json")
                .send()
                .await
            {
                if let Ok(text) = result.text().await {
                    if let Ok(account_info) = serde_json::from_str::<AccountInfo>(&text) {
                        // The websocket account notification has a
                        // different format from the getAccountInfo RPC
                        // endpoint. Reformat everything to look like the
                        // websocket format. Downstream clients will only
                        // subscribe to the websocket.
                        let notification = AccountNotification {
                            jsonrpc: account_info.jsonrpc,
                            method: Method::accountNotification,
                            params: NotificationParams {
                                result: account_info.result,
                                subscription: account_info.id,
                            },
                        };
                        // Let the query fail since we will retry momentarily.
                        // TODO: Limit retries, throttle.
                        if let Err(err) = self
                            .send
                            .send(EndpointToServer::AccountNotification(notification))
                        {
                            error!(
                                http_endpoint = self.url.as_str(),
                                poll_failure = err.to_string().as_str()
                            );
                        }
                    }
                }
            }
            interval.tick().await;
        }
    }
}
