use crate::{
    account_subscription::AccountSubscriptionHandler,
    channel_types::*,
    client::ClientHandler,
    endpoint::{self, EndpointConfig},
    jsonrpc,
    subscription_handler::SubscriptionHandler,
};
use hyper::service::{make_service_fn, service_fn};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

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

    account_subscriptions: AccountSubscriptionHandler,
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
            account_subscriptions: AccountSubscriptionHandler::new(),
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
                self.account_subscriptions.unsubscribe_client(
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

    fn on_endpoint_message(&mut self, message: EndpointToServer) {
        match message {
            EndpointToServer::Notification(notification) => {
                self.process_notification(notification);
            }
        }
    }

    fn process_notification(&mut self, notification: jsonrpc::Notification) {
        match notification.method.as_str() {
            "accountNotification" => {
                self.account_subscriptions
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

    fn send_string(send_to_client: UnboundedSender<ServerToClient>, message: String) {
        if let Err(err) = send_to_client.send(ServerToClient::Message(message)) {
            error!("server to client channel failure: {}", err);
        }
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
