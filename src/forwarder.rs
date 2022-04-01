use crate::messages::*;
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result},
    {MaybeTlsStream, WebSocketStream},
};
use url::Url;

/// Manages communication with a single rpc endpoint.
pub struct Forwarder {
    websocket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    client: Arc<reqwest::Client>,
    rpc_url: Option<Url>,
    subscription_to_id: HashMap<u64, u64>,
}

impl Forwarder {
    pub async fn new(websocket_url: Option<Url>, rpc_url: Option<Url>) -> Result<Self> {
        let websocket = if let Some(websocket_url) = websocket_url {
            let (websocket, _) = connect_async(websocket_url).await?;
            Some(websocket)
        } else {
            None
        };

        Ok(Forwarder {
            websocket,
            client: Arc::new(reqwest::Client::new()),
            rpc_url,
            subscription_to_id: HashMap::new(),
        })
    }

    /// Main event loop. A single task manages the websocket I/O. A task per
    /// subscription manages the synchronous RPC calls.
    pub async fn run(
        &mut self,
        send_to_multiplexer: UnboundedSender<AccountNotification>,
        mut receive_from_multiplexer: UnboundedReceiver<Instruction>,
    ) {
        let mut interval = time::interval(std::time::Duration::from_secs(10));

        // Since self.websocket is Optional, we cannot cleanly use a single
        // tokio select. Unwrapping the websocket panics, even if we gate the
        // selection by an if statement.
        if self.websocket.is_some() {
            loop {
                tokio::select! {
                    // Process new subscriptions first, since these should be
                    // relatively rare. Send the subscription as-is.
                    Some(instruction) = receive_from_multiplexer.recv() => {
                        self.websocket.as_mut().unwrap().send(Message::from(serde_json::to_string(&instruction).unwrap())).await.unwrap();

                        // Regularly make synchronous requests and arbitrate with
                        // the websocket. Note that the instruction id that was sent
                        // by the multiplexer is the global counter that the
                        // multiplexer expects to receive back. This means we can
                        // send the instruction as-is since the rpc server will echo
                        // back the id.
                        if let Some(ref mut rpc_url) = self.rpc_url {
                            Self::create_synchronous_subscription(self.client.clone(), rpc_url.clone(), &instruction, &send_to_multiplexer);
                        }
                    }

                    // If any data is available to be read, process it next.
                    item = self.websocket.as_mut().unwrap().next() => {
                        if let Some(item) = item {
                            let data = item.unwrap().into_text().unwrap();
                            if let Ok(reply) = serde_json::from_str::<SubscriptionReply>(&data) {
                                self.on_subscription_reply(&reply);
                            } else if let Ok(reply) = serde_json::from_str::<AccountNotification>(&data) {
                                self.on_account_notification(&reply, &send_to_multiplexer);
                            }
                        }
                    }

                    // Send a keepalive message.
                    _ = interval.tick(), if self.websocket.as_mut().is_some() => {
                        self.websocket.as_mut().unwrap().send(Message::from("{}")).await.unwrap();
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    // Process new subscriptions first, since these should be
                    // relatively rare. Send the subscription as-is.
                    Some(instruction) = receive_from_multiplexer.recv() => {
                        // Regularly make synchronous requests and arbitrate with
                        // the websocket. Note that the instruction id that was sent
                        // by the multiplexer is the global counter that the
                        // multiplexer expects to receive back. This means we can
                        // send the instruction as-is since the rpc server will echo
                        // back the id.
                        if let Some(ref mut rpc_url) = self.rpc_url {
                            Self::create_synchronous_subscription(self.client.clone(), rpc_url.clone(), &instruction, &send_to_multiplexer);
                        }
                    }
                }
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
    ) {
        let mut instruction = instruction.clone();
        instruction.method = Method::getAccountInfo;
        let instruction = serde_json::to_string(&instruction).unwrap();
        let send_to_multiplexer = send_to_multiplexer.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(std::time::Duration::from_secs(1));
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

    /// Registers the subscription to the multiplexer id. Note that the same
    /// subscription may correspond to multiple ids. The multiplexer is
    /// responsible for forwarding to all interested parties.
    fn on_subscription_reply(&mut self, reply: &SubscriptionReply) {
        self.subscription_to_id.insert(reply.result, reply.id);
    }

    /// Forwards the account notification to the multiplexer. The subscription
    /// is replaced with the ID that the multiplexer originally passed to
    /// initiate the subscription. Note that the same subscription may
    /// correspond to multiple ids. The multiplexer is responsible for
    /// forwarding messages to all interested parties.
    fn on_account_notification(
        &mut self,
        reply: &AccountNotification,
        send_to: &UnboundedSender<AccountNotification>,
    ) {
        if let Some(id) = self.subscription_to_id.get(&reply.params.subscription) {
            let mut reply = reply.clone();
            reply.params.subscription = *id;

            if send_to.send(reply).is_err() {
                // TODO: Some error handling when we can't write to client.
            }
        }
    }
}
