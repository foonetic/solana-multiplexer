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
    subscriptions: Arc<Mutex<HashMap<u64, HashSet<u64>>>>,
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
    next_subscription: u64,
    pubkey_to_subscription: HashMap<String, u64>,
    subscriptions: Arc<Mutex<HashMap<u64, HashSet<u64>>>>,
    endpoints: Vec<UnboundedSender<ServerToEndpoint>>,
}

impl Server {
    /// Initializes the server to connect to the given endpoints. The server
    /// will listen at the input address.
    pub async fn new(config: &[EndpointConfig], address: &str) -> Self {
        let (client_to_server, receive_from_clients) = unbounded_channel();
        let (receive_from_endpoints, endpoints) = endpoint::spawn(config).await;
        let client_senders = spawn_port_listener(address, client_to_server).await;
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        spawn_arbitrator(
            receive_from_endpoints,
            client_senders.clone(),
            subscriptions.clone(),
        );

        Server {
            receive_from_clients,
            client_senders,
            next_subscription: 0,
            pubkey_to_subscription: HashMap::new(),
            subscriptions,
            endpoints,
        }
    }

    /// Server event loop that continuously polls for new clients.
    pub async fn run(&mut self) {
        while let Some(from_client) = self.receive_from_clients.recv().await {
            match from_client {
                ClientToServer::RemoveClient(client) => {
                    let mut clients = self.client_senders.lock().unwrap();
                    clients.remove(&client);
                    info!(remove_client_id = client);

                    // TODO: Unsubscribe if the client list is empty.
                }

                ClientToServer::Instruction(client, instruction) => {
                    self.process_instruction(client, &instruction);
                }
            }
        }
    }

    /// Processes instructions sent from clients. The server is responsible for
    /// mapping client ids to globally unique ids before forwarding to
    /// endpoints.
    fn process_instruction(&mut self, client: u64, instruction: &Instruction) {
        match instruction.method {
            Method::accountSubscribe => {
                let pubkey = instruction.get_pubkey();
                let original_id = instruction.id;

                // Check that the instruction includes a PubKey.
                if let Some(pubkey) = pubkey {
                    // If the subscription for that PubKey already exists, then
                    // return the current subscription. Otherwise, add a new
                    // subscription.
                    let subscription =
                        if let Some(subscription) = self.pubkey_to_subscription.get(&pubkey) {
                            *subscription
                        } else {
                            let subscription = self.next_subscription;
                            self.pubkey_to_subscription.insert(pubkey, subscription);
                            self.next_subscription += 1;
                            let mut instruction = instruction.clone();
                            instruction.id = subscription;
                            for endpoint in self.endpoints.iter() {
                                // TODO: How should we handle channel write failures?
                                if let Err(err) = endpoint
                                    .send(ServerToEndpoint::Instruction(instruction.clone()))
                                {
                                    error!(endpoint_channel_failure = err.to_string().as_str());
                                }
                            }
                            subscription
                        };

                    let got = {
                        let clients = self.client_senders.lock().unwrap();
                        clients.get(&client).map(|v| v.clone())
                    };
                    if let Some(sender) = got {
                        // Add the client to the current list of subscribers.
                        {
                            let mut subscriptions = self.subscriptions.lock().unwrap();
                            subscriptions
                                .entry(subscription)
                                .or_insert(HashSet::new())
                                .insert(client);
                        }
                        info!(client_id = client, subscription_id = subscription);

                        // TODO: How should we handle channel write failures?
                        if let Err(err) =
                            sender.send(ServerToClient::SubscriptionReply(SubscriptionReply {
                                jsonrpc: "2.0".to_string(),
                                result: subscription,
                                id: original_id,
                            }))
                        {
                            error!(
                                client_id = client,
                                subscription_reply_channel_error = err.to_string().as_str()
                            );
                        }
                    }
                } else {
                    // Send an error message back to the client.
                    let got = {
                        let clients = self.client_senders.lock().unwrap();
                        clients.get(&client).map(|v| v.clone())
                    };
                    if let Some(sender) = got {
                        // TODO: How should we handle channel write failures?
                        if let Err(err) = sender.send(ServerToClient::Error(Error {
                            jsonrpc: "2.0".to_string(),
                            code: ErrorCode::InvalidParams,
                            message: "unable to parse PublicKey".to_string(),
                        })) {
                            error!(
                                client_id = client,
                                subscription_reply_channel_error = err.to_string().as_str()
                            );
                        }
                    }
                }
            }

            _ => {}
        }
    }
}
