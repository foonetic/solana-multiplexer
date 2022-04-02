use crate::{messages::*, multiplexer::Endpoint};
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result},
};
use url::Url;

/// Manages communication with a single rpc endpoint.
pub struct Forwarder {
    interface: Interface,
}

enum Interface {
    Rpc((Arc<reqwest::Client>, Url, std::time::Duration)),
    WebSocket(
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ),
}

impl Forwarder {
    pub async fn new(endpoint: &Endpoint) -> Result<Self> {
        let interface = match endpoint {
            Endpoint::Rpc(url, poll_frequency) => Interface::Rpc((
                Arc::new(reqwest::Client::new()),
                url.clone(),
                poll_frequency.clone(),
            )),
            Endpoint::WebSocket(url) => {
                let (websocket, _) = connect_async(url).await?;
                Interface::WebSocket(websocket)
            }
        };

        Ok(Forwarder { interface })
    }

    /// Main event loop. A single task manages the websocket I/O. A task per
    /// subscription manages the synchronous RPC calls.
    pub async fn run(
        &mut self,
        send_to_multiplexer: UnboundedSender<AccountNotification>,
        receive_from_multiplexer: UnboundedReceiver<Instruction>,
    ) {
        match &mut self.interface {
            &mut Interface::Rpc((ref client, ref url, ref poll_frequency)) => {
                Self::rpc_event_loop(
                    client,
                    url,
                    poll_frequency,
                    send_to_multiplexer,
                    receive_from_multiplexer,
                )
                .await;
            }

            &mut Interface::WebSocket(ref mut client) => {
                Self::websocket_event_loop(client, send_to_multiplexer, receive_from_multiplexer)
                    .await;
            }
        }
    }

    /// Launches a task that polls the rpc endpoint for account information
    /// every second. The resulting data is arbitrated against websocket
    /// subscription data and formatted as websocket data before being sent to
    /// the client.
    fn create_synchronous_subscription(
        client: Arc<reqwest::Client>,
        rpc_url: Url,
        instruction: &Instruction,
        send_to_multiplexer: &UnboundedSender<AccountNotification>,
        poll_frequency: &std::time::Duration,
    ) {
        let mut instruction = instruction.clone();
        instruction.method = Method::getAccountInfo;
        let instruction = serde_json::to_string(&instruction).unwrap();
        let send_to_multiplexer = send_to_multiplexer.clone();
        let poll_frequency = poll_frequency.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(poll_frequency);
            loop {
                interval.tick().await;
                if let Ok(result) = client
                    .post(rpc_url.clone())
                    .body(instruction.clone())
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

                            // Multiplexer arbitrates the rpc endpoint data
                            // against the websocket.
                            send_to_multiplexer.send(notification).unwrap();
                        }
                    }
                }
            }
        });
    }

    async fn rpc_event_loop(
        client: &Arc<reqwest::Client>,
        url: &Url,
        poll_frequency: &std::time::Duration,
        send_to_multiplexer: UnboundedSender<AccountNotification>,
        mut receive_from_multiplexer: UnboundedReceiver<Instruction>,
    ) {
        loop {
            // Process new subscriptions first, since these should be
            // relatively rare. Send the subscription as-is.
            if let Some(instruction) = receive_from_multiplexer.recv().await {
                // Regularly make synchronous requests and arbitrate with
                // the websocket. Note that the instruction id that was sent
                // by the multiplexer is the global counter that the
                // multiplexer expects to receive back. This means we can
                // send the instruction as-is since the rpc server will echo
                // back the id.
                Self::create_synchronous_subscription(
                    client.clone(),
                    url.clone(),
                    &instruction,
                    &send_to_multiplexer,
                    &poll_frequency,
                );
            }
        }
    }

    async fn websocket_event_loop(
        client: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        send_to_multiplexer: UnboundedSender<AccountNotification>,
        mut receive_from_multiplexer: UnboundedReceiver<Instruction>,
    ) {
        let mut subscription_to_id = HashMap::new();
        let mut interval = time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                // Process new subscriptions first, since these should be
                // relatively rare. Send the subscription as-is.
                Some(instruction) = receive_from_multiplexer.recv() => {
                    client.send(Message::from(serde_json::to_string(&instruction).unwrap())).await.unwrap();
                }

                // If any data is available to be read, process it next.
                item = client.next() => {
                    Self::process_websocket_data(item, &mut subscription_to_id, send_to_multiplexer.clone());
                }

                // Send a keepalive message.
                _ = interval.tick() => {
                    client.send(Message::from("{}")).await.unwrap();
                }
            }
        }
    }

    fn process_websocket_data(
        item: Option<Result<Message>>,
        subscription_to_id: &mut HashMap<u64, u64>,
        send_to_multiplexer: UnboundedSender<AccountNotification>,
    ) {
        if let Some(item) = item {
            let data = item.unwrap().into_text().unwrap();
            if let Ok(reply) = serde_json::from_str::<SubscriptionReply>(&data) {
                subscription_to_id.insert(reply.result, reply.id);
            } else if let Ok(reply) = serde_json::from_str::<AccountNotification>(&data) {
                if let Some(id) = subscription_to_id.get(&reply.params.subscription) {
                    let mut reply = reply.clone();
                    reply.params.subscription = *id;
                    if send_to_multiplexer.send(reply).is_err() {
                        // TODO: Some error handling when we can't write to client.
                    }
                }
            }
        }
    }
}
