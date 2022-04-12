use crate::{
    channel_types::*,
    client::ClientHandler,
    endpoint::{self, EndpointConfig},
    jsonrpc, metrics,
    subscriptions::{
        AccountSubscriptionHandler, LogsSubscriptionHandler, ProgramSubscriptionHandler,
        RootSubscriptionHandler, SignatureSubscriptionHandler, SlotSubscriptionHandler,
        SubscriptionHandler,
    },
};
use hyper::{
    service::{make_service_fn, service_fn},
    Response, StatusCode,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

/// Implements a subset of a Solana PubSub endpoint supporting HTTP and PubSub
/// API calls.
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

    account_subscriptions: AccountSubscriptionHandler,
    logs_subscriptions: LogsSubscriptionHandler,
    program_subscriptions: ProgramSubscriptionHandler,
    signature_subscriptions: SignatureSubscriptionHandler,
    slot_subscriptions: SlotSubscriptionHandler,
    root_subscriptions: RootSubscriptionHandler,
}

impl Server {
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
            account_subscriptions: AccountSubscriptionHandler::new(),
            logs_subscriptions: LogsSubscriptionHandler::new(),
            program_subscriptions: ProgramSubscriptionHandler::new(),
            signature_subscriptions: SignatureSubscriptionHandler::new(),
            slot_subscriptions: SlotSubscriptionHandler::new(),
            root_subscriptions: RootSubscriptionHandler::new(),
        }
    }

    /// Runs the main event loop. Connects to the listed endpoints and serves
    /// the API at the given address.
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

    /// Processes one event in the main event loop.
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

    /// Starts listening at the given address for clients.
    async fn spawn_listener(&self, address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let address: std::net::SocketAddr = address.parse()?;
        let send_to_server = self.client_to_server.clone();

        let make_service = make_service_fn(move |_| {
            // Called once per connection.
            let send_to_server = send_to_server.clone();

            async move {
                // Called once per request.
                let send_to_server = send_to_server.clone();

                Ok::<_, hyper::Error>(service_fn(move |request| {
                    metrics::CLIENT_REQUEST_COUNT.inc();

                    // Returns the future that should be executed to get a response.
                    let client_handler = if request.uri().path() == "/metrics" {
                        None
                    } else {
                        Some(ClientHandler::new(send_to_server.clone()))
                    };
                    async move {
                        match request.uri().path() {
                            "/metrics" => metrics::serve_metrics(),
                            "/" => client_handler.unwrap().run(request).await,
                            _ => Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(hyper::Body::empty())
                                .map_err(|e| e.into()),
                        }
                    }
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

    /// Initializes connections to all configured endpoints.
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
                        url.clone(),
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

    /// Processes a client message.
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
                self.account_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
                self.logs_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
                self.program_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
                self.signature_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
                self.slot_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
                self.root_subscriptions.unsubscribe_client(
                    client.clone(),
                    &mut self.next_instruction_id,
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }

            ClientToServer::PubSubRequest { client, request } => {
                self.process_pubsub(client, request);
            }

            ClientToServer::HTTPRequest { client, request } => {
                self.process_http(client, request);
            }
        }
    }

    /// Processes an endpoint message.
    fn on_endpoint_message(&mut self, message: EndpointToServer) {
        match message {
            EndpointToServer::Notification(notification, url) => {
                self.process_notification(notification, url);
            }
        }
    }

    /// Processes an endpoint message that was determined to be a notification.
    fn process_notification(&mut self, notification: jsonrpc::Notification, source: url::Url) {
        match notification.method.as_str() {
            "accountNotification" => {
                if let Some(slot) =
                    AccountSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.account_subscriptions
                    .broadcast(notification, &self.send_to_client);
            }
            "logsNotification" => {
                if let Some(slot) =
                    LogsSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.logs_subscriptions
                    .broadcast(notification, &self.send_to_client);
            }
            "programNotification" => {
                if let Some(slot) =
                    ProgramSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.program_subscriptions
                    .broadcast(notification, &self.send_to_client);
            }
            "signatureNotification" => {
                if let Some(slot) =
                    SignatureSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.signature_subscriptions
                    .broadcast_and_unsubscribe(notification, &self.send_to_client);
            }
            "slotNotification" => {
                if let Some(slot) =
                    SlotSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.slot_subscriptions
                    .broadcast(notification, &self.send_to_client);
            }
            "rootNotification" => {
                if let Some(slot) =
                    RootSubscriptionHandler::get_notification_timestamp(&notification)
                {
                    metrics::LATEST_SLOT_SEEN
                        .with_label_values(&[source.as_str()])
                        .set(slot as i64);
                }
                self.root_subscriptions
                    .broadcast(notification, &self.send_to_client);
            }
            unknown => {
                info!(
                    "saw unknown notification method {}: {:?}",
                    unknown, notification,
                );
            }
        }
    }

    /// Processes a client pubsub request.
    fn process_pubsub(&mut self, client: ClientID, request: jsonrpc::Request) {
        let send_to_client = self.send_to_client.get(&client);
        if send_to_client.is_none() {
            return;
        }
        let send_to_client = send_to_client.unwrap().clone();

        match request.method.as_str() {
            "accountSubscribe" => {
                self.account_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "accountUnsubscribe" => {
                self.account_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }
            "logsSubscribe" => {
                self.logs_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "logsUnsubscribe" => {
                self.logs_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }
            "programSubscribe" => {
                self.program_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "programUnsubscribe" => {
                self.program_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }
            "signatureSubscribe" => {
                self.signature_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "signatureUnsubscribe" => {
                self.signature_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }
            "slotSubscribe" => {
                self.slot_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "slotUnsubscribe" => {
                self.slot_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
            }
            "rootSubscribe" => {
                self.root_subscriptions.subscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    &self.send_to_http.as_slice(),
                );
            }
            "rootUnsubscribe" => {
                self.root_subscriptions.unsubscribe(
                    client,
                    &request,
                    &mut self.next_instruction_id,
                    send_to_client.clone(),
                    self.send_to_pubsub.as_slice(),
                    self.send_to_http.as_slice(),
                );
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

    /// Processes a client HTTP request.
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

    /// Sends a string message to the client.
    fn send_string(send_to_client: UnboundedSender<ServerToClient>, message: String) {
        if let Err(err) = send_to_client.send(ServerToClient::Message(message)) {
            error!("server to client channel failure: {}", err);
        }
    }

    /// Sends an error message to the client.
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
