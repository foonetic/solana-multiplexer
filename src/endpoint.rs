use crate::{channel_types::*, jsonrpc, metrics};
use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
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

/// Represents a local subscriber ID. This is very likely to be a different
/// number from the server-provided instruction ID. The endpoint is responsible
/// for mapping the subscriber back to the global ID in all notification sent to
/// the server.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EndpointSubscriberID(i64);

/// Represents a PubSub endpont.
pub struct PubsubEndpoint {
    url: Url,
    reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    enqueue: UnboundedSender<Message>,
    send_to_server: UnboundedSender<EndpointToServer>,
    receive_from_server: UnboundedReceiver<ServerToPubsub>,
    global_to_local_subscriber: HashMap<ServerInstructionID, EndpointSubscriberID>,
    local_to_global_subscriber: HashMap<EndpointSubscriberID, ServerInstructionID>,
}

impl PubsubEndpoint {
    pub fn new(
        url: Url,
        ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
        send_to_server: UnboundedSender<EndpointToServer>,
        receive_from_server: UnboundedReceiver<ServerToPubsub>,
    ) -> Self {
        let (mut writer, reader) = ws.split();
        let (enqueue, mut dequeue) = unbounded_channel();

        tokio::spawn(async move {
            while let Some(message) = dequeue.recv().await {
                if let Err(err) = writer.send(message).await {
                    error!(error_writing_to_client = err.to_string().as_str());
                    return;
                }
            }
        });

        Self {
            url,
            reader,
            enqueue,
            send_to_server,
            receive_from_server,
            global_to_local_subscriber: HashMap::new(),
            local_to_global_subscriber: HashMap::new(),
        }
    }

    /// Event loop for the PubSub endpoint.
    pub async fn run(&mut self) {
        let mut interval = time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                Some(message) = self.receive_from_server.recv() => {
                    self.on_instruction(message);
                }

                message = self.reader.next() => {
                    if message.is_none() {
                        return;
                    }
                    let message = message.unwrap();
                    if message.is_err() {
                        return;
                    }
                    self.on_message(message.unwrap());
                }

                _ = interval.tick() => {
                    if let Err(err) = self.enqueue.send(Message::Ping(vec![0])) {
                        error!("pubsub endpoint failed to ping: {}", err);
                    }
                }
            }
        }
    }

    /// Handles a websocket event.
    fn on_message(&mut self, message: Message) {
        match message {
            Message::Text(msg) => self.on_text_message(msg),
            Message::Binary(msg) => self.on_binary_message(msg),
            Message::Ping(_msg) => {
                // No need to send a reply: tungstenite takes care of this for you.
                metrics::PUBSUB_MESSAGE_COUNT
                    .with_label_values(&[self.url.as_str(), "ping"])
                    .inc();
            }
            Message::Pong(_msg) => {
                // Ignore: assume all pongs are valid.
                metrics::PUBSUB_MESSAGE_COUNT
                    .with_label_values(&[self.url.as_str(), "pong"])
                    .inc();
            }
            Message::Close(_msg) => {
                // No need to send reply; this is handled by the library.
                metrics::PUBSUB_MESSAGE_COUNT
                    .with_label_values(&[self.url.as_str(), "close"])
                    .inc();
            }
            Message::Frame(_msg) => {
                unreachable!();
            }
        }
    }

    /// Handles a text websocket event.
    fn on_text_message(&mut self, message: String) {
        if let Ok(reply) = serde_json::from_str(&message) {
            self.on_json(reply);
        } else {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "unknown_text"])
                .inc();
        }
    }

    /// Handles a binary websocket event.
    fn on_binary_message(&mut self, message: Vec<u8>) {
        if let Ok(reply) = serde_json::from_slice(&message) {
            self.on_json(reply);
        } else {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "unknown_binary"])
                .inc();
        }
    }

    /// Handles a json websocket event.
    fn on_json(&mut self, value: serde_json::Value) {
        if let Ok(mut notification) = serde_json::from_value::<jsonrpc::Notification>(value.clone())
        {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "notification"])
                .inc();

            if let Some(id) = self
                .local_to_global_subscriber
                .get(&EndpointSubscriberID(notification.params.subscription))
            {
                notification.params.subscription = id.0;
                if let Err(err) = self.send_to_server.send(EndpointToServer::Notification(
                    notification,
                    self.url.clone(),
                )) {
                    error!(
                        "unable to enqueue notification from pubsub endpoint: {}",
                        err
                    );
                }
            } else {
                metrics::PUBSUB_MESSAGE_COUNT
                    .with_label_values(&[self.url.as_str(), "unknown_notification"])
                    .inc();
            }
        } else if let Ok(reply) =
            serde_json::from_value::<jsonrpc::SubscribeResponse>(value.clone())
        {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "subscribe"])
                .inc();
            self.on_subscribe_reply(reply);
        } else if let Ok(reply) =
            serde_json::from_value::<jsonrpc::UnsubscribeResponse>(value.clone())
        {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "unsubscribe"])
                .inc();
            info!("unsubscribe confirmed for request {}", reply.id);
        } else {
            metrics::PUBSUB_MESSAGE_COUNT
                .with_label_values(&[self.url.as_str(), "unknown"])
                .inc();
            info!("unhandled reply: {:?}", value);
        }
    }

    /// Handles a subscriptiohn reply. Maps the subscription ID to the global
    /// unique ID that was used in the subscription request.
    fn on_subscribe_reply(&mut self, reply: jsonrpc::SubscribeResponse) {
        self.global_to_local_subscriber.insert(
            ServerInstructionID(reply.id),
            EndpointSubscriberID(reply.result),
        );
        self.local_to_global_subscriber.insert(
            EndpointSubscriberID(reply.result),
            ServerInstructionID(reply.id),
        );
    }

    /// Handles an instruction from the server.
    fn on_instruction(&mut self, message: ServerToPubsub) {
        match message {
            ServerToPubsub::Subscribe {
                subscription,
                request,
            } => {
                info!(
                    "pubsub endpoint subscribing to global id {}",
                    subscription.0
                );
                if let Err(err) = self.enqueue.send(Message::Text(request)) {
                    error!("failed to enqueue pubsub write: {}", err);
                }
            }

            ServerToPubsub::Unsubscribe {
                request_id,
                subscription,
                method,
            } => {
                let entry = self.global_to_local_subscriber.entry(subscription.clone());
                match entry {
                    Entry::Occupied(got) => {
                        info!(
                            "pubsub endpoint unsubscribing to global id {}",
                            subscription.0,
                        );
                        let instruction = format!(
                            r#"{{"jsonrpc":"2.0","id":{},"method":"{}","params":[{}]}}"#,
                            request_id.0,
                            method,
                            got.get().0,
                        );
                        if let Err(err) = self.enqueue.send(Message::Text(instruction)) {
                            error!("failed to enqueue pubsub write: {}", err);
                        }
                        self.local_to_global_subscriber.remove(&got.remove());
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }
    }
}

/// Regularly polls an HTTP endpoint at some configured frequency.
struct HTTPPoll {
    client: Arc<reqwest::Client>,
    url: Url,
    frequency: Duration,
    instruction: String,
    unsubscribe: oneshot::Receiver<bool>,
    send: UnboundedSender<EndpointToServer>,
    method: String,
}

impl HTTPPoll {
    fn new(
        client: Arc<reqwest::Client>,
        url: Url,
        frequency: Duration,
        instruction: String,
        unsubscribe: oneshot::Receiver<bool>,
        send: UnboundedSender<EndpointToServer>,
        method: String,
    ) -> Self {
        HTTPPoll {
            client,
            url,
            frequency,
            instruction,
            unsubscribe,
            send,
            method,
        }
    }

    /// Repeatedly sends HTTP requests until the subscription is removed.
    async fn run(&mut self) {
        let mut interval = time::interval(self.frequency);
        loop {
            tokio::select! {
                Ok(_) = &mut self.unsubscribe => {
                    info!("http endpoint {} halting poll", self.url.as_str());
                    return;
                }
                _ = interval.tick() => {
                    self.query().await;
                }
            }
        }
    }

    /// Sends a single HTTP request.
    async fn query(&mut self) {
        if let Ok(result) = self
            .client
            .post(self.url.clone())
            .body(self.instruction.clone())
            .header("content-type", "application/json")
            .send()
            .await
        {
            if let Ok(text) = result.text().await {
                if let Ok(response) = serde_json::from_str::<jsonrpc::Response>(&text) {
                    // Reformat to look like a websocket reply.
                    let notification = jsonrpc::Notification {
                        jsonrpc: response.jsonrpc,
                        method: self.method.clone(),
                        params: jsonrpc::NotificationParams {
                            result: response.result,
                            subscription: response.id,
                        },
                    };
                    // Let the query fail since we will retry momentarily.
                    if let Err(err) = self.send.send(EndpointToServer::Notification(
                        notification,
                        self.url.clone(),
                    )) {
                        error!(
                            "http endpoint {} failed to poll: {}",
                            self.url.as_str(),
                            err
                        );
                    }
                }
            }
        }
    }
}

/// Represents an HTTP endpoint that can either be polled or used to send single
/// requests.
pub struct HTTPEndpoint {
    client: Arc<reqwest::Client>,
    url: Url,
    frequency: Duration,
    send_to_server: UnboundedSender<EndpointToServer>,
    receive_from_server: UnboundedReceiver<ServerToHTTP>,
    account_unsubscribe: HashMap<ServerInstructionID, oneshot::Sender<bool>>,
}

impl HTTPEndpoint {
    pub fn new(
        client: Arc<reqwest::Client>,
        url: Url,
        frequency: Duration,
        send_to_server: UnboundedSender<EndpointToServer>,
        receive_from_server: UnboundedReceiver<ServerToHTTP>,
    ) -> Self {
        Self {
            client,
            url,
            frequency,
            send_to_server,
            receive_from_server,
            account_unsubscribe: HashMap::new(),
        }
    }

    /// Main event loop.
    pub async fn run(&mut self) {
        loop {
            self.poll().await;
        }
    }

    /// Process one event in the event loop.
    async fn poll(&mut self) {
        tokio::select! {
            Some(message) = self.receive_from_server.recv() => {
                self.on_message(message);
            }
        }
    }

    /// Processes a message from the server. Handles subscribe, unsubscribe, and
    /// direct HTTP requests.
    fn on_message(&mut self, message: ServerToHTTP) {
        match message {
            ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            } => {
                info!(
                    "http endpoint {} subscribing to global id {}",
                    self.url.as_str(),
                    subscription.0,
                );
                let (send_unsubscribe, receive_unsubscribe) = oneshot::channel();
                self.account_unsubscribe
                    .insert(subscription.clone(), send_unsubscribe);
                let mut poll = HTTPPoll::new(
                    self.client.clone(),
                    self.url.clone(),
                    self.frequency.clone(),
                    request,
                    receive_unsubscribe,
                    self.send_to_server.clone(),
                    method,
                );
                tokio::spawn(async move {
                    poll.run().await;
                });
            }

            ServerToHTTP::Unsubscribe(subscription) => {
                info!(
                    "http endpoint {} unsubscribing to global id {}",
                    self.url.as_str(),
                    subscription.0,
                );

                let entry = self.account_unsubscribe.entry(subscription);
                match entry {
                    Entry::Occupied(got) => {
                        if let Err(err) = got.remove().send(true) {
                            error!("failed to send http unsubscribe through channel: {}", err);
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }

            ServerToHTTP::DirectRequest(request, sender) => {
                info!("http endpoint {} issuing direct request", self.url.as_str(),);
                let client = self.client.clone();
                let url = self.url.clone();
                let body =
                    serde_json::to_string(&request).expect("unable to serialize json request");
                tokio::spawn(async move {
                    if let Ok(result) = client
                        .post(url)
                        .body(body)
                        .header("content-type", "application/json")
                        .send()
                        .await
                    {
                        if let Ok(text) = result.text().await {
                            if let Err(err) = sender.send(ServerToClient::Message(text)) {
                                error!("direct http request failed: {}", err);
                            }
                        }
                    }
                });
            }
        }
    }
}
