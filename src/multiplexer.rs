use crate::{forwarder::*, messages::*};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tokio_tungstenite::tungstenite::{Message, Result};
use url::Url;

/// Provides a websocket endpoint that dispatches replies to multiple websocket
/// and rpc endpoints. Responses are arbitrated by slot number and the most
/// recent data is returned to the user.
pub struct Multiplexer {
    // Tracks underlying endpoints.
    forwarders: Vec<Forwarder>,

    // Ensures that subscriptions to different forwarders can be managed by a
    // single logical key. Each forwarder may have a different internal
    // subscription id. The forwarder is responsible for mapping that
    // subscription id back to this global counter before sending back to the
    // multiplexer.
    global_counter: Arc<Mutex<u64>>,

    // Maps from a global counter to potentially multple subscribers. Each
    // subscriber is represented by a tuple consisting of that subscriber's
    // request id and that subscriber's client id. The request id is a
    // user-supplied identifier. The client id is a multiplexer-generated
    // identifier unique to the client's connection.
    //
    // This mapping is used to dispatch a single subscription across multiple
    // subscribers. For example, if client A and client B both subscribe to key
    // XYZ, and client A uses id 123 and client B uses id 456, this mapping will
    // take a global identifier, say 789, and forward the responses to the
    // downstream clients.
    global_counter_to_subscription: Arc<Mutex<HashMap<u64, HashSet<(u64, u64)>>>>,

    // Maps from a subscribed public key to the global counter that uniquely
    // represents it.
    pubkey_to_global_counter: Arc<Mutex<HashMap<String, u64>>>,

    // Maps from a multiplexer-generated client identifier to a sender that
    // passes data to the client websocket.
    client_id_to_send: Arc<Mutex<HashMap<u64, UnboundedSender<AccountNotification>>>>,
}

/// Represents an RPC endpoint. Generally speaking, we should expect both RPC
/// and websocket endpoints to exist for a logical endpoint. The RPC endpoint
/// will be polled synchronously and arbitrated against the websocket. It is
/// possible to disable either of these sources.
pub enum Endpoint {
    /// JSON RPC endpoint with a polling frequencye.
    Rpc(Url, std::time::Duration),

    /// Streaming endpoint.
    WebSocket(Url),
}

impl Multiplexer {
    /// Connects to the given endpoints.
    pub async fn new(endpoints: &[Endpoint]) -> Result<Self> {
        let mut forwarders = Vec::new();

        for endpoint in endpoints.iter() {
            let node = Forwarder::new(endpoint.clone()).await?;
            forwarders.push(node);
        }

        Ok(Multiplexer {
            forwarders,
            global_counter: Arc::new(Mutex::new(0)),
            global_counter_to_subscription: Arc::new(Mutex::new(HashMap::new())),
            pubkey_to_global_counter: Arc::new(Mutex::new(HashMap::new())),
            client_id_to_send: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Serves a websocket server at the input address.
    pub async fn listen(&mut self, addr: &str) {
        let (send_account_notification, receive_account_notification) = unbounded_channel();

        let send_instructions = self.run_communication_with_forwarders(&send_account_notification);

        let mut return_arbitrator = ReturnArbitrator {
            global_counter_to_subscription: self.global_counter_to_subscription.clone(),
            client_id_to_send: self.client_id_to_send.clone(),
            receive_account_notification: receive_account_notification,
        };
        tokio::spawn(async move {
            return_arbitrator.run().await;
        });

        self.run_websockset_server(send_instructions, addr).await;
    }

    /// Creates the event loops for relaying between the multiplexer and the
    /// forwarder instances.
    fn run_communication_with_forwarders(
        &mut self,
        send_account_notification: &UnboundedSender<AccountNotification>,
    ) -> Vec<UnboundedSender<Instruction>> {
        let mut senders = Vec::new();
        for mut forwarder in self.forwarders.drain(0..) {
            let (send_instruction, receive_instruction) = unbounded_channel();
            senders.push(send_instruction);
            let send_account_notification = send_account_notification.clone();
            tokio::spawn(async move {
                forwarder
                    .run(send_account_notification, receive_instruction)
                    .await;
            });
        }

        senders
    }

    /// Creates the websocket server event loop.
    async fn run_websockset_server(
        &self,
        send_instructions: Vec<UnboundedSender<Instruction>>,
        addr: &str,
    ) {
        let listener = TcpListener::bind(addr).await.unwrap();
        let mut client_id: u64 = 0;

        // Maps between the globally unique client id and the function that is
        // used to send account notifications to that account.
        let send_instructions = Arc::new(send_instructions);

        while let Ok((stream, _)) = listener.accept().await {
            let (send_to_client, receive_for_client) = unbounded_channel();
            let send_instructions = send_instructions.clone();

            // Track the channel for each client.
            {
                self.client_id_to_send
                    .lock()
                    .unwrap()
                    .insert(client_id, send_to_client);
            }

            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();

            let mut client_handler = ClientHandler {
                client_id,
                ws_stream,
                pubkey_to_global_counter: self.pubkey_to_global_counter.clone(),
                global_counter_to_subscription: self.global_counter_to_subscription.clone(),
                global_counter: self.global_counter.clone(),
                send_instructions: send_instructions.clone(),
                receive_for_client,
            };
            tokio::spawn(async move {
                client_handler.run().await;
            });
            client_id += 1;
        }
    }
}

/// Runs the event loop for arbitrating messages. Arbitration is keyed by slot
/// number for a fixed subscription identifier. The subscription id is
/// guaranteed by the forwarders to be a global unique id passed from the
/// multiplexer.
struct ReturnArbitrator {
    global_counter_to_subscription: Arc<Mutex<HashMap<u64, HashSet<(u64, u64)>>>>,
    client_id_to_send: Arc<Mutex<HashMap<u64, UnboundedSender<AccountNotification>>>>,
    receive_account_notification: UnboundedReceiver<AccountNotification>,
}

impl ReturnArbitrator {
    async fn run(&mut self) {
        let mut slot_by_subscription = HashMap::new();
        while let Some(result) = self.receive_account_notification.recv().await {
            // Send if the update slot number is more recent than any previously
            // sent update.
            let should_send =
                if let Some(slot) = slot_by_subscription.get(&result.params.subscription) {
                    if result.params.result.context.slot > *slot {
                        true
                    } else {
                        false
                    }
                } else {
                    true
                };

            if should_send {
                // Track the most recently sent.
                slot_by_subscription.insert(
                    result.params.subscription,
                    result.params.result.context.slot,
                );

                // The multiplexer guarantees that each pubkey is subscribed to by at
                // most one subscribe call. On the call, the subscription id is
                // guaranteed to be a global_counter value. Map that back to the clients
                // we need to send to.
                let global_counter_to_subscription =
                    self.global_counter_to_subscription.lock().unwrap();
                let client_id_to_send = self.client_id_to_send.lock().unwrap();
                if let Some(ref subscribers) =
                    global_counter_to_subscription.get(&result.params.subscription)
                {
                    // Each underlying client may have invoked the subscription
                    // with a different id. Keep that id consistent.
                    for (client_instruction_id, client_id) in subscribers.iter() {
                        if let Some(sender) = client_id_to_send.get(client_id) {
                            let mut result = result.clone();
                            result.params.subscription = *client_instruction_id;
                            if sender.send(result).is_err() {
                                // TODO: Error handling when unable to send to
                                // client. For now, this is handled further
                                // upstream in the multiplexer.
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Handles an inbound client connection. Relays instructions to all forwarders
/// and passes arbitrated notifications back to the client.
struct ClientHandler {
    client_id: u64,
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    pubkey_to_global_counter: Arc<Mutex<HashMap<String, u64>>>,
    global_counter_to_subscription: Arc<Mutex<HashMap<u64, HashSet<(u64, u64)>>>>,
    global_counter: Arc<Mutex<u64>>,
    send_instructions: Arc<Vec<UnboundedSender<Instruction>>>,
    receive_for_client: UnboundedReceiver<AccountNotification>,
}

impl ClientHandler {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                // User sent a request. Dispatch it to each of the underlying
                // nodes. Use the same global counter for all of the nodes so
                // that we can map it back to the originating request.
                item = self.ws_stream.next() => {
                    if let Some(item) = item {
                        let data = item;
                        if data.is_err() {
                            // TODO: Error handling when reading from client is interrupted.
                            return;
                        }
                        let data = data.unwrap().into_text();
                        if data.is_err() {
                            // TODO: Error handling when reading from client is interrupted.
                            return;
                        }
                        let data = data.unwrap();
                        if let Ok(instruction) = serde_json::from_str::<Instruction>(&data) {
                            self.maybe_add_subscriber(&instruction, instruction.id);
                        } else {
                            let err = Error {
                                jsonrpc: "2.0".to_string(),
                                code: ErrorCode::InvalidRequest,
                                message: data.to_string(),
                            };
                            let err = serde_json::to_string(&err).unwrap();
                            self.ws_stream.send(Message::from(err)).await.unwrap();
                        }
                    }
                }

                // We received a notification that needs to be sent to
                // the client. The multiplexer is responsible for
                // setting the subscription correctly.
                Some(notification) = self.receive_for_client.recv() => {
                    self.ws_stream.send(Message::from(serde_json::to_string(&notification).unwrap())).await.unwrap();
                }
            }
        }
    }

    /// Adds a subscriber if the pubkey isn't already a subscription. If it is a
    /// subscription, there's no need to relay a new subscription. Instead, the
    /// client will be relayed the existing subscription data.
    fn maybe_add_subscriber(&mut self, instruction: &Instruction, client_instruction_id: u64) {
        if let Some(pubkey) = instruction.get_pubkey() {
            let mut pubkey_to_global_counter = self.pubkey_to_global_counter.lock().unwrap();
            if let Some(global_counter) = pubkey_to_global_counter.get(&pubkey) {
                // The pubkey is already subscribed to. Add this client to the list of
                // clients we should broadcast to, but don't repeat the subscription.
                let mut global_counter_to_subscription =
                    self.global_counter_to_subscription.lock().unwrap();
                if let Some(set) = global_counter_to_subscription.get_mut(global_counter) {
                    set.insert((client_instruction_id, self.client_id));
                }
            } else {
                // The pubkey isn't subscribed to.
                let mut set = HashSet::new();
                set.insert((client_instruction_id, self.client_id));
                let global_counter = {
                    let mut global_counter = self.global_counter.lock().unwrap();
                    pubkey_to_global_counter.insert(pubkey.to_string(), *global_counter);
                    *global_counter += 1;
                    *global_counter - 1
                };

                {
                    let mut global_counter_to_subscription =
                        self.global_counter_to_subscription.lock().unwrap();
                    global_counter_to_subscription.insert(global_counter, set);
                }

                // Dispatch to the underlying forwarders.
                let mut instruction = instruction.clone();
                instruction.id = global_counter;
                for sender in self.send_instructions.iter() {
                    sender.send(instruction.clone()).unwrap();
                }
            }
        }
    }
}
