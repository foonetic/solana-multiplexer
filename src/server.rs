use crate::{
    channel_types::*,
    client::ClientHandler,
    endpoint::{self, EndpointConfig},
    jsonrpc,
};
use hyper::service::{make_service_fn, service_fn};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

#[derive(Clone, Debug, Eq)]
struct Subscriber {
    client: ClientID,
    encoding: Encoding,
}

impl Hash for Subscriber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.client.hash(state);
    }
}

impl PartialEq for Subscriber {
    fn eq(&self, other: &Self) -> bool {
        self.client == other.client
    }
}

struct AccountSubscriptionTracker {
    client_to_subscriptions: HashMap<ClientID, HashSet<ServerInstructionID>>,
    subscription_to_clients: HashMap<ServerInstructionID, HashSet<Subscriber>>,
    account_notifications: HashMap<ServerInstructionID, u64>,
}

impl AccountSubscriptionTracker {
    fn new() -> Self {
        Self {
            client_to_subscriptions: HashMap::new(),
            subscription_to_clients: HashMap::new(),
            account_notifications: HashMap::new(),
        }
    }

    fn track_subscriber(
        &mut self,
        client: &ClientID,
        subscription: &ServerInstructionID,
        subscriber: Subscriber,
    ) {
        match self.client_to_subscriptions.entry(client.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(subscription.clone());
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(subscription.clone());
                entry.insert(set);
            }
        }

        match self.subscription_to_clients.entry(subscription.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(subscriber);
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(subscriber);
                entry.insert(set);
            }
        }
    }

    /// Returns true if the notification should be broadcasted. This is the case
    /// if the subscription is new or has a later slot than any existing notification.
    fn notification_is_most_recent(&mut self, notification: &jsonrpc::AccountNotification) -> bool {
        let latest = self
            .account_notifications
            .entry(ServerInstructionID(notification.params.subscription));
        match latest {
            Entry::Vacant(entry) => {
                entry.insert(notification.params.result.context.slot);
                true
            }
            Entry::Occupied(mut existing) => {
                if *existing.get() < notification.params.result.context.slot {
                    existing.insert(notification.params.result.context.slot);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn get_notification_subscribers(
        &self,
        notification: &jsonrpc::AccountNotification,
    ) -> Option<&HashSet<Subscriber>> {
        self.subscription_to_clients
            .get(&ServerInstructionID(notification.params.subscription))
    }

    /// Removes a single subscription for a client. Returns true if that
    /// subscription should be removed globally or false if it should remain.
    /// Returns None if the client wasn't subscribed.
    fn remove_single_subscription(
        &mut self,
        client: &ClientID,
        subscription: &ServerInstructionID,
    ) -> Option<bool> {
        Self::remove_single_subscription_from_map(
            &mut self.subscription_to_clients,
            client,
            subscription,
        )
    }

    fn remove_single_subscription_from_map(
        subscription_to_clients: &mut HashMap<ServerInstructionID, HashSet<Subscriber>>,
        client: &ClientID,
        subscription: &ServerInstructionID,
    ) -> Option<bool> {
        if let Entry::Occupied(mut entry) = subscription_to_clients.entry(subscription.clone()) {
            let subscribed_clients = entry.get_mut();
            if !subscribed_clients.remove(&Subscriber {
                client: client.clone(),
                encoding: Encoding::Base58, // Arbitrary; not hashed or compared.
            }) {
                return None;
            }
            if subscribed_clients.len() == 0 {
                entry.remove();

                // There aren't any subscribers remaining.
                return Some(true);
            } else {
                // There are still subscribers remaining.
                return Some(false);
            }
        } else {
            None
        }
    }

    /// Removes a client and returns all subscriptions that should be
    /// unsubscribed as a result.
    fn remove_client(&mut self, client: &ClientID) -> Vec<ServerInstructionID> {
        let mut to_unsubscribe = Vec::new();
        if let Some(subscriptions) = self.client_to_subscriptions.get(client) {
            for subscription in subscriptions.iter() {
                if let Some(should_remove) = Self::remove_single_subscription_from_map(
                    &mut self.subscription_to_clients,
                    client,
                    &subscription.clone(),
                ) {
                    if should_remove {
                        to_unsubscribe.push(subscription.clone());
                    }
                }
            }
        }
        self.client_to_subscriptions.remove(&client);
        to_unsubscribe
    }
}

/// Implements a subset of a Solana PubSub endpoint that multiplexes across
/// various endpoints.
pub struct Server {
    next_client_id: ClientID,
    next_instruction_id: ServerInstructionID,
    next_http_endpoint: u64,
    client_to_server: UnboundedSender<ClientToServer>,
    receive_from_client: UnboundedReceiver<ClientToServer>,
    endpoint_to_server: UnboundedSender<EndpointToServer>,
    receive_from_endpoint: UnboundedReceiver<EndpointToServer>,
    send_to_http: Vec<UnboundedSender<ServerToHTTP>>,
    send_to_pubsub: Vec<UnboundedSender<ServerToPubsub>>,
    send_to_client: HashMap<ClientID, UnboundedSender<ServerToClient>>,

    account_subscription_tracker: AccountSubscriptionTracker,
    pubkey_to_subscription: HashMap<String, ServerInstructionID>,
    subscription_to_pubkey: HashMap<ServerInstructionID, String>,
}

impl Server {
    /// Initializes the server to connect to the given endpoints. The server
    /// will listen at the input address.
    pub fn new() -> Self {
        let (client_to_server, receive_from_client) = unbounded_channel();
        let (endpoint_to_server, receive_from_endpoint) = unbounded_channel();
        Server {
            next_client_id: ClientID(0),
            next_instruction_id: ServerInstructionID(0),
            next_http_endpoint: 0,
            client_to_server,
            receive_from_client,
            endpoint_to_server,
            receive_from_endpoint,
            send_to_http: Vec::new(),
            send_to_pubsub: Vec::new(),
            send_to_client: HashMap::new(),
            account_subscription_tracker: AccountSubscriptionTracker::new(),
            pubkey_to_subscription: HashMap::new(),
            subscription_to_pubkey: HashMap::new(),
        }
    }

    /// Server event loop that continuously polls for new clients.
    pub async fn run(
        &mut self,
        config: &[EndpointConfig],
        address: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.spawn_endpoints(config).await;
        self.spawn_listener(address).await?;

        loop {
            self.poll().await;
        }
    }

    async fn poll(&mut self) {
        tokio::select! {
            Some(from_client) = self.receive_from_client.recv() => {
                self.on_client_message(from_client);
            }
            Some(from_endpoint) = self.receive_from_endpoint.recv() => {
                self.on_endpoint_message(from_endpoint);
            }
        }
    }

    async fn spawn_listener(&self, address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let address: std::net::SocketAddr = address.parse()?;
        let send_to_server = self.client_to_server.clone();

        let make_service = make_service_fn(move |_| {
            // Called once per connection.
            let send_to_server = send_to_server.clone();

            async move {
                // Called once per request.

                Ok::<_, hyper::Error>(service_fn(move |request| {
                    let mut client_handler = ClientHandler::new(send_to_server.clone());

                    // Returns the future that should be executed to get a response.
                    async move { client_handler.run(request).await }
                }))
            }
        });

        tokio::spawn(async move {
            if let Err(err) = hyper::Server::bind(&address).serve(make_service).await {
                error!("unable to serve: {}", err);
            }
        });
        Ok(())
    }

    async fn spawn_endpoints(&mut self, config: &[EndpointConfig]) {
        let client = Arc::new(reqwest::Client::new());
        for endpoint in config.iter() {
            match endpoint {
                EndpointConfig::HTTP(url, frequency) => {
                    let (server_to_endpoint, receive_from_server) = unbounded_channel();
                    info!("connecting to HTTP endpoint {}", url.as_str());
                    let mut http_endpoint = endpoint::HTTPEndpoint::new(
                        client.clone(),
                        url.clone(),
                        frequency.clone(),
                        self.endpoint_to_server.clone(),
                        receive_from_server,
                    );
                    tokio::spawn(async move {
                        http_endpoint.run().await;
                    });
                    self.send_to_http.push(server_to_endpoint);
                }

                EndpointConfig::PubSub(url) => {
                    let (server_to_endpoint, receive_from_server) = unbounded_channel();
                    info!("connecting to Pubsub endpoint {}", url.as_str());
                    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
                    let mut pubsub_endpoint = endpoint::PubsubEndpoint::new(
                        ws,
                        self.endpoint_to_server.clone(),
                        receive_from_server,
                    );
                    tokio::spawn(async move {
                        pubsub_endpoint.run().await;
                    });
                    self.send_to_pubsub.push(server_to_endpoint);
                }
            }
        }
    }

    fn on_client_message(&mut self, message: ClientToServer) {
        match message {
            ClientToServer::Initialize(sender) => {
                let id = self.next_client_id.clone();
                self.next_client_id.0 += 1;
                // If the client already disconnected, do nothing.
                if let Ok(_) = sender.send(ServerToClient::Initialize(id.clone())) {
                    info!("initializing new client with id {}", id.0);
                    self.send_to_client.insert(id, sender);
                }
            }

            ClientToServer::RemoveClient(client) => {
                self.process_remove_client(client);
            }

            ClientToServer::PubSubRequest { client, request } => {
                self.process_pubsub(client, request);
            }

            ClientToServer::HTTPRequest { client, request } => {
                self.process_http(client, request);
            }
        }
    }

    fn on_endpoint_message(&mut self, message: EndpointToServer) {
        match message {
            EndpointToServer::AccountNotification(notification) => {
                self.process_account_notification(notification);
            }
        }
    }

    fn process_account_notification(&mut self, notification: jsonrpc::AccountNotification) {
        if self
            .account_subscription_tracker
            .notification_is_most_recent(&notification)
        {
            self.broadcast_account_notification(&notification);
        }
    }

    fn broadcast_account_notification(&mut self, notification: &jsonrpc::AccountNotification) {
        let data = notification.params.result.value.data.get(0);
        if data.is_none() {
            return;
        }
        let data = data.unwrap();

        if let Some(subscribers) = self
            .account_subscription_tracker
            .get_notification_subscribers(notification)
        {
            let mut cache: HashMap<Encoding, String> = HashMap::new();
            for subscriber in subscribers.iter() {
                let mut raw_bytes = None;

                if let Some(sender) = self.send_to_client.get(&subscriber.client) {
                    match cache.entry(subscriber.encoding.clone()) {
                        Entry::Occupied(element) => {
                            Self::send_string(sender.clone(), element.get().clone());
                        }
                        Entry::Vacant(element) => {
                            let notification = match subscriber.encoding {
                                Encoding::Base58 => {
                                    if raw_bytes.is_none() {
                                        let decode = base64::decode(data);
                                        if decode.is_err() {
                                            return;
                                        }
                                        raw_bytes = Some(decode.unwrap());
                                    }
                                    let data_string =
                                        bs58::encode(&raw_bytes.unwrap()).into_string();
                                    let mut notification = notification.clone();
                                    if let Some(loc) =
                                        notification.params.result.value.data.get_mut(0)
                                    {
                                        *loc = data_string;
                                    }
                                    if let Some(enc) =
                                        notification.params.result.value.data.get_mut(1)
                                    {
                                        *enc = "base58".to_string();
                                    } else {
                                        notification
                                            .params
                                            .result
                                            .value
                                            .data
                                            .push("base58".to_string());
                                    }
                                    notification
                                }
                                Encoding::Base64 => notification.clone(),
                                Encoding::Base64Zstd => {
                                    if raw_bytes.is_none() {
                                        let decode = base64::decode(data);
                                        if decode.is_err() {
                                            return;
                                        }
                                        raw_bytes = Some(decode.unwrap());
                                    }
                                    let res =
                                        zstd::stream::encode_all(raw_bytes.unwrap().as_slice(), 0);
                                    if res.is_err() {
                                        return;
                                    }
                                    let res = res.unwrap();
                                    let data_string = base64::encode(&res);
                                    let mut notification = notification.clone();
                                    if let Some(loc) =
                                        notification.params.result.value.data.get_mut(0)
                                    {
                                        *loc = data_string;
                                    }
                                    if let Some(enc) =
                                        notification.params.result.value.data.get_mut(1)
                                    {
                                        *enc = "base64+zstd".to_string();
                                    } else {
                                        notification
                                            .params
                                            .result
                                            .value
                                            .data
                                            .push("base64+zstd".to_string());
                                    }
                                    notification
                                }
                            };
                            let value = serde_json::to_string(&notification)
                                .expect("unable to serialize account notification");
                            Self::send_string(sender.clone(), value.clone());
                            element.insert(value);
                        }
                    }
                }
            }
        }
    }

    fn process_remove_client(&mut self, client: ClientID) {
        self.send_to_client.remove(&client);
        info!("removing client {}", client.0);

        let mut to_unsubscribe = self.account_subscription_tracker.remove_client(&client);
        for unsubscribe in to_unsubscribe.drain(0..) {
            self.remove_global_subscription(&unsubscribe);
            if let Entry::Occupied(pubkey) = self.subscription_to_pubkey.entry(unsubscribe) {
                self.pubkey_to_subscription.remove(&pubkey.remove());
            }
        }
    }

    /// Unsubscribes from a global subscription ID. This does *NOT* check that
    /// all clients have been removed! Caller is responsible for ensuring that
    /// the subscription is indeed no longer needed.
    fn remove_global_subscription(&mut self, unsubscribe: &ServerInstructionID) {
        let instruction_id = self.next_instruction_id.clone();
        self.next_instruction_id.0 += 1;

        for endpoint in self.send_to_http.iter() {
            if let Err(err) = endpoint.send(ServerToHTTP::AccountUnsubscribe(unsubscribe.clone())) {
                error!("server to http channel failure: {}", err);
            }
        }

        for endpoint in self.send_to_pubsub.iter() {
            if let Err(err) = endpoint.send(ServerToPubsub::AccountUnsubscribe {
                request_id: instruction_id.clone(),
                subscription: unsubscribe.clone(),
            }) {
                error!("server to pubsub channel failure: {}", err);
            }
        }
    }

    fn process_pubsub(&mut self, client: ClientID, request: jsonrpc::Request) {
        let send_to_client = self.send_to_client.get(&client);
        if send_to_client.is_none() {
            return;
        }
        let send_to_client = send_to_client.unwrap().clone();

        match request.method.as_str() {
            "accountSubscribe" => {
                self.process_account_subscribe(send_to_client.clone(), client, request);
            }
            "accountUnsubscribe" => {
                self.process_account_unsubscribe(send_to_client.clone(), client, request);
            }
            unknown => {
                Self::send_error(
                    send_to_client.clone(),
                    jsonrpc::ErrorCode::MethodNotFound,
                    format!("unknown method {}", unknown),
                    request.id,
                );
            }
        }
    }

    fn process_http(&mut self, client: ClientID, request: jsonrpc::Request) {
        // The client may have already disconnected. In that case, skip the request.
        if let Some(send_to_client) = self.send_to_client.get(&client) {
            if self.send_to_http.len() == 0 {
                Self::send_error(
                    send_to_client.clone(),
                    jsonrpc::ErrorCode::InternalError,
                    "no HTTP endpoints defined".to_string(),
                    request.id,
                );
                return;
            }
            // Round robin the HTTP endpoint.
            let endpoint = self.next_http_endpoint % self.send_to_http.len() as u64;
            self.next_http_endpoint = self.next_http_endpoint.wrapping_add(1);
            let request_id = request.id;

            // This check covers the corner case where there are no HTTP endpoints.
            if let Some(endpoint) = self.send_to_http.get(endpoint as usize) {
                // Let the HTTP endpoint directly send the response back to the
                // client, since no arbitration is needed.
                if let Err(err) =
                    endpoint.send(ServerToHTTP::DirectRequest(request, send_to_client.clone()))
                {
                    Self::send_error(
                        send_to_client.clone(),
                        jsonrpc::ErrorCode::InternalError,
                        format!("send to HTTP channel failed: {}", err),
                        request_id,
                    );
                }
            } else {
                if self.send_to_http.len() == 0 {
                    Self::send_error(
                        send_to_client.clone(),
                        jsonrpc::ErrorCode::InternalError,
                        "unable to communicate with endpoint".to_string(),
                        request.id,
                    );
                    return;
                }
            }

            return;
        }
        info!("client {} disconnected", client.0);
    }

    fn process_account_unsubscribe(
        &mut self,
        send_to_client: UnboundedSender<ServerToClient>,
        client: ClientID,
        request: jsonrpc::Request,
    ) {
        let mut to_unsubscribe = None;
        if let serde_json::Value::Array(params) = request.params {
            if params.len() == 1 {
                if let Some(serde_json::Value::Number(num)) = params.get(0) {
                    to_unsubscribe = num.as_i64();
                }
            }
        }

        if to_unsubscribe.is_none() {
            Self::send_error(
                send_to_client.clone(),
                jsonrpc::ErrorCode::InvalidParams,
                "unable to parse unsubscribe parameters".to_string(),
                request.id,
            );
            return;
        }
        let to_unsubscribe = ServerInstructionID(to_unsubscribe.unwrap());

        if let Some(should_remove_globally) = self
            .account_subscription_tracker
            .remove_single_subscription(&client, &to_unsubscribe)
        {
            info!(
                "pubsub client {} unsubscribing to {}",
                client.0, to_unsubscribe.0
            );
            Self::send_unsubscribe_response(send_to_client.clone(), request.id);
            if should_remove_globally {
                self.remove_global_subscription(&to_unsubscribe);
            }
        } else {
            Self::send_error(
                send_to_client.clone(),
                jsonrpc::ErrorCode::InvalidRequest,
                "not currently subscribed".to_string(),
                request.id,
            );
            return;
        }
    }

    fn process_account_subscribe(
        &mut self,
        send_to_client: UnboundedSender<ServerToClient>,
        client: ClientID,
        request: jsonrpc::Request,
    ) {
        if let serde_json::Value::Array(mut params) = request.params {
            if let Some(serde_json::Value::String(pubkey)) = params.get(0) {
                let pubkey = pubkey.clone();
                let encoding = if let Some(serde_json::Value::String(val)) =
                    params.get_mut(1).and_then(|obj| {
                        if let serde_json::Value::Object(obj) = obj {
                            obj.get_mut("encoding")
                        } else {
                            None
                        }
                    }) {
                    let original = val.clone();
                    *val = "base64".to_string();
                    original
                } else {
                    "base64".to_string()
                };

                let recognized_encoding;
                match encoding.as_str() {
                    "base58" => recognized_encoding = Encoding::Base58,
                    "base64" => recognized_encoding = Encoding::Base64,
                    "base64+zstd" => recognized_encoding = Encoding::Base64Zstd,
                    _ => {
                        Self::send_error(
                            send_to_client,
                            jsonrpc::ErrorCode::InvalidParams,
                            format!("unrecognized encoding {}", encoding),
                            request.id,
                        );
                        return;
                    }
                }

                // Update bidirectional pubkey mapping.
                let subscription = self.pubkey_to_subscription.entry(pubkey.clone());
                let subscription_id = match subscription {
                    Entry::Occupied(existing) => existing.get().clone(),
                    Entry::Vacant(put) => {
                        let current_id = self.next_instruction_id.clone();
                        self.next_instruction_id.0 += 1;
                        put.insert(current_id.clone());

                        // In this branch of the code, we need to send a new subscription request.
                        for endpoint in self.send_to_pubsub.iter() {
                            endpoint
                                .send(ServerToPubsub::AccountSubscribe {
                                    subscription: current_id.clone(),
                                    pubkey: pubkey.clone(),
                                })
                                .expect("pubsub endpoint channel died");
                        }
                        for endpoint in self.send_to_http.iter() {
                            endpoint
                                .send(ServerToHTTP::AccountSubscribe {
                                    subscription: current_id.clone(),
                                    pubkey: pubkey.clone(),
                                })
                                .expect("http endpoint channel died");
                        }
                        current_id
                    }
                };

                info!(
                    "pubsub client {} subscribing to {} as global id {}",
                    client.0, &pubkey, subscription_id.0
                );

                self.subscription_to_pubkey
                    .insert(subscription_id.clone(), pubkey.clone());

                // Update bidirectional client mapping.
                let subscriber = Subscriber {
                    client: client.clone(),
                    encoding: recognized_encoding,
                };
                self.account_subscription_tracker.track_subscriber(
                    &client,
                    &subscription_id,
                    subscriber,
                );

                Self::send_subscription_response(send_to_client, request.id, subscription_id.0);
            } else {
                Self::send_error(
                    send_to_client,
                    jsonrpc::ErrorCode::InvalidParams,
                    "no public key provided".to_string(),
                    request.id,
                );
            }
        } else {
            Self::send_error(
                send_to_client,
                jsonrpc::ErrorCode::InvalidParams,
                "subscription takes array parameter".to_string(),
                request.id,
            );
        }
    }

    fn send_string(send_to_client: UnboundedSender<ServerToClient>, message: String) {
        if let Err(err) = send_to_client.send(ServerToClient::Message(message)) {
            error!("server to client channel failure: {}", err);
        }
    }

    fn send_subscription_response(
        send_to_client: UnboundedSender<ServerToClient>,
        id: i64,
        result: i64,
    ) {
        let json = serde_json::to_string(&jsonrpc::SubscribeResponse {
            jsonrpc: "2.0".to_string(),
            result,
            id,
        })
        .expect("unable to serialize subscription reply to json");
        Self::send_string(send_to_client, json);
    }

    fn send_unsubscribe_response(send_to_client: UnboundedSender<ServerToClient>, id: i64) {
        let json = serde_json::to_string(&jsonrpc::UnsubscribeResponse {
            jsonrpc: "2.0".to_string(),
            result: true,
            id,
        })
        .expect("unable to serialize unsubscribe reply to json");
        Self::send_string(send_to_client, json);
    }

    fn send_error(
        send_to_client: UnboundedSender<ServerToClient>,
        code: jsonrpc::ErrorCode,
        message: String,
        id: i64,
    ) {
        let json = serde_json::to_string(&jsonrpc::Error {
            jsonrpc: "2.0".to_string(),
            code,
            message,
            id,
        })
        .expect("unable to serialize error to json");
        Self::send_string(send_to_client, json);
    }
}
