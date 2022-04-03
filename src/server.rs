use crate::{
    arbitrator::EndpointArbitrator,
    endpoint::{self, EndpointConfig},
    listener::PortListener,
    messages::*,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::{error, info};

/// Spawns an event loop that arbitrates messages as they arrive from various
/// endpoints. The messages from the endpoints are assumed to have their
/// subscription ids already corrected to the global unique id.
fn spawn_arbitrator(
    receive_from_endpoints: UnboundedReceiver<EndpointToServer>,
    client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
    subscriptions: Arc<Mutex<HashMap<i64, HashSet<u64>>>>,
) {
    let mut arbitrator =
        EndpointArbitrator::new(receive_from_endpoints, client_senders, subscriptions);
    tokio::spawn(async move {
        arbitrator.run().await;
    });
}

/// Spawns an event loop that initializes new client connections.
async fn spawn_port_listener(
    address: &str,
    client_to_server: UnboundedSender<ClientToServer>,
) -> Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>> {
    let clients = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind(address).await.unwrap();
    let mut client_listener = PortListener::new(clients.clone(), client_to_server);
    tokio::spawn(async move {
        client_listener.run(listener).await;
    });

    clients
}

/// Implements a subset of a Solana PubSub endpoint that multiplexes across
/// various endpoints.
pub struct Server {
    receive_from_clients: UnboundedReceiver<ClientToServer>,
    client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
    next_instruction_id: i64,
    pubkey_to_subscription: HashMap<String, i64>,
    subscription_to_client: Arc<Mutex<HashMap<i64, HashSet<u64>>>>,
    client_to_subscription: Arc<Mutex<HashMap<u64, HashSet<i64>>>>,
    endpoints: Vec<UnboundedSender<ServerToEndpoint>>,
}

impl Server {
    /// Initializes the server to connect to the given endpoints. The server
    /// will listen at the input address.
    pub async fn new(config: &[EndpointConfig], address: &str) -> Self {
        let (client_to_server, receive_from_clients) = unbounded_channel();
        let (receive_from_endpoints, endpoints) = endpoint::spawn(config).await;
        let client_senders = spawn_port_listener(address, client_to_server).await;
        let subscription_to_client = Arc::new(Mutex::new(HashMap::new()));
        let client_to_subscription = Arc::new(Mutex::new(HashMap::new()));
        spawn_arbitrator(
            receive_from_endpoints,
            client_senders.clone(),
            subscription_to_client.clone(),
        );

        Server {
            receive_from_clients,
            client_senders,
            next_instruction_id: 1,
            pubkey_to_subscription: HashMap::new(),
            subscription_to_client,
            client_to_subscription,
            endpoints,
        }
    }

    /// Server event loop that continuously polls for new clients.
    pub async fn run(&mut self) {
        while let Some(from_client) = self.receive_from_clients.recv().await {
            match from_client {
                ClientToServer::RemoveClient(client) => {
                    self.process_remove_client(client);
                }

                ClientToServer::Instruction(client, instruction) => {
                    self.process_instruction(client, &instruction);
                }
            }
        }
    }

    fn process_remove_client(&mut self, client: u64) {
        let mut clients = self.client_senders.lock().unwrap();
        clients.remove(&client);
        info!(remove_client_id = client);

        let mut client_to_subscription = self.client_to_subscription.lock().unwrap();
        let mut subscription_to_client = self.subscription_to_client.lock().unwrap();
        let mut to_unsubscribe = Vec::new();
        if let Some(subscriptions) = client_to_subscription.get(&client) {
            for subscription in subscriptions.iter() {
                if let Some(subscribed_clients) = subscription_to_client.get_mut(&subscription) {
                    subscribed_clients.remove(&client);
                    if subscribed_clients.len() == 0 {
                        to_unsubscribe.push(subscription);
                    }
                }
            }
        }
        for unsubscribe in to_unsubscribe.iter() {
            subscription_to_client.remove(unsubscribe);

            for endpoint in self.endpoints.iter() {
                // TODO: How should we handle channel write failures?

                if let Err(err) = endpoint.send(ServerToEndpoint::Instruction(Instruction {
                    jsonrpc: "2.0".to_string(),
                    id: self.next_instruction_id,
                    method: Method::accountUnsubscribe,
                    params: serde_json::Value::Array(vec![serde_json::Value::Number(
                        serde_json::Number::from(**unsubscribe),
                    )]),
                })) {
                    error!(endpoint_channel_failure = err.to_string().as_str());
                }
            }

            // TODO: We should also remove the pubkey from the tracked mapping.
            // For now, cache the pubkey so all subscriptions in one multiplexer
            // instance will have a consistent identifier.

            self.next_instruction_id += 1;
        }
        client_to_subscription.remove(&client);
    }

    /// Processes instructions sent from clients. The server is responsible for
    /// mapping client ids to globally unique ids before forwarding to
    /// endpoints.
    fn process_instruction(&mut self, client: u64, instruction: &Instruction) {
        match instruction.method {
            Method::accountSubscribe => {
                self.process_account_subscribe(client, instruction);
            }

            _ => self.send_error(
                client,
                ErrorCode::InvalidRequest,
                "unsupported request".to_string(),
                instruction.id,
            ),
        }
    }

    fn process_account_subscribe(&mut self, client: u64, instruction: &Instruction) {
        let pubkey = instruction.get_pubkey();

        // Check that the instruction includes a PubKey.
        if let Some(pubkey) = pubkey {
            self.process_account_subscribe_with_pubkey(client, instruction, pubkey);
        } else {
            self.send_pubkey_missing_error(client, instruction.id);
        }
    }

    fn send_error(&mut self, client: u64, code: ErrorCode, message: String, id: i64) {
        // Send an error message back to the client.
        let got = {
            let clients = self.client_senders.lock().unwrap();
            clients.get(&client).map(|v| v.clone())
        };
        if let Some(sender) = got {
            // TODO: How should we handle channel write failures?
            if let Err(err) = sender.send(ServerToClient::Error(Error {
                jsonrpc: "2.0".to_string(),
                code,
                message,
                id,
            })) {
                error!(
                    client_id = client,
                    subscription_reply_channel_error = err.to_string().as_str()
                );
            }
        }
    }

    fn send_pubkey_missing_error(&mut self, client: u64, id: i64) {
        self.send_error(
            client,
            ErrorCode::InvalidParams,
            "unable to parse PublicKey".to_string(),
            id,
        );
    }

    fn process_account_subscribe_with_pubkey(
        &mut self,
        client: u64,
        instruction: &Instruction,
        pubkey: String,
    ) {
        // If the subscription for that PubKey already exists, then
        // return the current subscription. Otherwise, add a new
        // subscription.
        let subscription = if let Some(subscription) = self.pubkey_to_subscription.get(&pubkey) {
            *subscription
        } else {
            self.process_new_pubkey_subscription(instruction, pubkey)
        };

        self.add_subscriber(client, subscription, instruction.id)
    }

    fn process_new_pubkey_subscription(
        &mut self,
        instruction: &Instruction,
        pubkey: String,
    ) -> i64 {
        let subscription = self.next_instruction_id;
        self.pubkey_to_subscription.insert(pubkey, subscription);
        self.next_instruction_id += 1;
        let mut instruction = instruction.clone();
        instruction.id = subscription;
        for endpoint in self.endpoints.iter() {
            // TODO: How should we handle channel write failures?
            if let Err(err) = endpoint.send(ServerToEndpoint::Instruction(instruction.clone())) {
                error!(endpoint_channel_failure = err.to_string().as_str());
            }
        }
        subscription
    }

    fn add_subscriber(&mut self, client: u64, subscription: i64, original_id: i64) {
        let got = {
            let clients = self.client_senders.lock().unwrap();
            clients.get(&client).map(|v| v.clone())
        };
        if let Some(sender) = got {
            // Add the client to the current list of subscribers.
            {
                let mut subscriptions = self.subscription_to_client.lock().unwrap();
                subscriptions
                    .entry(subscription)
                    .or_insert(HashSet::new())
                    .insert(client);
            }

            // Add the subscription for the client.
            {
                let mut subscriptions = self.client_to_subscription.lock().unwrap();
                subscriptions
                    .entry(client)
                    .or_insert(HashSet::new())
                    .insert(subscription);
            }
            info!(client_id = client, subscription_id = subscription);

            // TODO: How should we handle channel write failures?
            if let Err(err) = sender.send(ServerToClient::SubscriptionReply(SubscriptionReply {
                jsonrpc: "2.0".to_string(),
                result: subscription,
                id: original_id,
            })) {
                error!(
                    client_id = client,
                    subscription_reply_channel_error = err.to_string().as_str()
                );
            }
        }
    }
}
