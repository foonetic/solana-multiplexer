/// Implements a simple Solana request multiplexer.
///
/// The multiplexer dispatches account subscriptions to multiple validators and
/// arbitrates responses to return the freshest data.
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result},
    {MaybeTlsStream, WebSocketStream},
};
use url::Url;

/// A websocket account notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountNotification {
    jsonrpc: String,
    method: String,
    params: NotificationParams,
}

/// A synchronous account info result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountInfo {
    jsonrpc: String,
    result: NotificationResult,
    id: u64,
}

/// Internal parameters within an account notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct NotificationParams {
    result: NotificationResult,
    subscription: u64,
}

/// Internal result within an account params.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct NotificationResult {
    context: NotificationContext,
    value: serde_json::Value,
}

/// Internal context within an account result.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct NotificationContext {
    slot: u64,
}

/// Manages communication with a single rpc endpoint.
struct Forwarder {
    websocket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    client: Arc<reqwest::Client>,
    rpc_url: Option<Url>,
    subscription_to_id: HashMap<u64, u64>,
}

impl Forwarder {
    async fn new(websocket_url: Option<Url>, rpc_url: Option<Url>) -> Result<Self> {
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
    async fn run(
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
        instruction.method = "getAccountInfo".to_string();
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
                                method: "accountSubscribe".to_string(),
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

/// Represents a websocket subscription response.
#[derive(Serialize, Deserialize)]
struct SubscriptionReply {
    result: u64,
    id: u64,
}

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
pub struct Endpoint {
    pub rpc: Option<Url>,
    pub websocket: Option<Url>,
}

impl Multiplexer {
    /// Connects to the given endpoints.
    pub async fn new(urls: &[Endpoint]) -> Result<Self> {
        let mut forwarders = Vec::new();

        for endpoint in urls.iter() {
            let node = Forwarder::new(endpoint.websocket.clone(), endpoint.rpc.clone()).await?;
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

        let global_counter_to_subscription = self.global_counter_to_subscription.clone();
        let client_id_to_send = self.client_id_to_send.clone();
        tokio::spawn(async move {
            Self::run_arbitrate_notifications(
                global_counter_to_subscription,
                client_id_to_send,
                receive_account_notification,
            )
            .await;
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
            let (send_to_client, mut receive_for_client) =
                unbounded_channel::<AccountNotification>();
            let send_instructions = send_instructions.clone();

            // Track the channel for each client.
            {
                self.client_id_to_send
                    .lock()
                    .unwrap()
                    .insert(client_id, send_to_client);
            }

            let pubkey_to_global_counter = self.pubkey_to_global_counter.clone();
            let global_counter_to_subscription = self.global_counter_to_subscription.clone();
            let global_counter = self.global_counter.clone();
            tokio::spawn(async move {
                let mut ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
                loop {
                    tokio::select! {
                        // User sent a request. Dispatch it to each of the underlying
                        // nodes. Use the same global counter for all of the nodes so
                        // that we can map it back to the originating request.
                        item = ws_stream.next() => {
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
                                    Self::maybe_add_subscriber(pubkey_to_global_counter.clone(),
                                    global_counter_to_subscription.clone(),
                                    global_counter.clone(), &instruction, instruction.id,
                                    client_id, &send_instructions);
                                }
                            }
                        }

                        // We received a notification that needs to be sent to
                        // the client. The multiplexer is responsible for
                        // setting the subscription correctly.
                        Some(notification) = receive_for_client.recv() => {
                            ws_stream.send(Message::from(serde_json::to_string(&notification).unwrap())).await.unwrap();
                        }
                    }
                }
            });
            client_id += 1;
        }
    }

    /// Adds a subscriber if the pubkey isn't already a subscription. If it is a
    /// subscription, there's no need to relay a new subscription. Instead, the
    /// client will be relayed the existing subscription data.
    fn maybe_add_subscriber(
        pubkey_to_global_counter: Arc<Mutex<HashMap<String, u64>>>,
        global_counter_to_subscription: Arc<Mutex<HashMap<u64, HashSet<(u64, u64)>>>>,
        global_counter: Arc<Mutex<u64>>,
        instruction: &Instruction,
        client_instruction_id: u64,
        client_id: u64,
        send_instructions: &[UnboundedSender<Instruction>],
    ) {
        if let Some(pubkey) = instruction.get_pubkey() {
            let mut pubkey_to_global_counter = pubkey_to_global_counter.lock().unwrap();
            if let Some(global_counter) = pubkey_to_global_counter.get(&pubkey) {
                // The pubkey is already subscribed to. Add this client to the list of
                // clients we should broadcast to, but don't repeat the subscription.
                let mut global_counter_to_subscription =
                    global_counter_to_subscription.lock().unwrap();
                if let Some(set) = global_counter_to_subscription.get_mut(global_counter) {
                    set.insert((client_instruction_id, client_id));
                }
            } else {
                // The pubkey isn't subscribed to.
                let mut set = HashSet::new();
                set.insert((client_instruction_id, client_id));
                let global_counter = {
                    let mut global_counter = global_counter.lock().unwrap();
                    pubkey_to_global_counter.insert(pubkey.to_string(), *global_counter);
                    *global_counter += 1;
                    *global_counter - 1
                };

                {
                    let mut global_counter_to_subscription =
                        global_counter_to_subscription.lock().unwrap();
                    global_counter_to_subscription.insert(global_counter, set);
                }

                // Dispatch to the underlying forwarders.
                let mut instruction = instruction.clone();
                instruction.id = global_counter;
                for sender in send_instructions.iter() {
                    sender.send(instruction.clone()).unwrap();
                }
            }
        }
    }

    /// Runs the event loop for arbitrating messages. Arbitration is keyed by
    /// slot number for a fixed subscription identifier. The subscription id is
    /// guaranteed by the forwarders to be a global unique id passed from the
    /// multiplexer.
    async fn run_arbitrate_notifications(
        global_counter_to_subscription: Arc<Mutex<HashMap<u64, HashSet<(u64, u64)>>>>,
        client_id_to_send: Arc<Mutex<HashMap<u64, UnboundedSender<AccountNotification>>>>,
        mut notifications: UnboundedReceiver<AccountNotification>,
    ) {
        let mut slot_by_subscription = HashMap::new();
        while let Some(result) = notifications.recv().await {
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
                let global_counter_to_subscription = global_counter_to_subscription.lock().unwrap();
                let client_id_to_send = client_id_to_send.lock().unwrap();
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

/// Represents an instruction sent to an endpoint.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Instruction {
    jsonrpc: String,
    id: u64,
    method: String,
    params: serde_json::Value,
}

impl Instruction {
    /// Returns the public key of the instruction. The public key is the first
    /// function parameter.
    fn get_pubkey(&self) -> Option<String> {
        if let serde_json::Value::Array(arr) = &self.params {
            if arr.len() > 0 {
                if let serde_json::Value::String(string) = &arr[0] {
                    return Some(string.clone());
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = vec![
        Endpoint {
            websocket: Some(Url::parse("wss://api.mainnet-beta.solana.com:443").unwrap()),
            rpc: Some(Url::parse("https://api.mainnet-beta.solana.com:443").unwrap()),
        },
        Endpoint {
            websocket: Some(Url::parse("wss://solana-api.projectserum.com:443").unwrap()),
            rpc: Some(Url::parse("https://solana-api.projectserum.com:443").unwrap()),
        },
    ];
    let mut multiplexer = Multiplexer::new(&urls).await.unwrap();
    multiplexer.listen("0.0.0.0:8900").await;

    Ok(())
}
