use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result},
    {MaybeTlsStream, WebSocketStream},
};
use url::Url;

#[derive(Serialize, Deserialize, Debug)]
struct AccountNotification {
    jsonrpc: String,
    method: String,
    params: NotificationParams,
}

#[derive(Serialize, Deserialize, Debug)]
struct NotificationParams {
    result: NotificationResult,
    subscription: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct NotificationResult {
    context: NotificationContext,
    value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct NotificationContext {
    slot: u64,
}

struct Node {
    write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    enqueue: UnboundedSender<std::string::String>,
    dequeue: UnboundedReceiver<std::string::String>,
    subscription_id_to_client_id: HashMap<u64, u64>,
    client_id_to_subscription_id: HashMap<u64, u64>,
}

impl Node {
    async fn new(websocket: &Url) -> Result<Self> {
        let (stream, _) = connect_async(websocket).await?;
        let (write, read) = stream.split();
        let (enqueue, dequeue) = unbounded_channel::<String>();
        Ok(Node {
            write,
            read,
            enqueue,
            dequeue,
            subscription_id_to_client_id: HashMap::new(),
            client_id_to_subscription_id: HashMap::new(),
        })
    }

    fn get_channel(&self) -> UnboundedSender<std::string::String> {
        self.enqueue.clone()
    }

    async fn run(&mut self, mut account_notifications: UnboundedSender<AccountNotification>) {
        let mut interval = time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                // Process new subsriptions first, since these should be relatively rare.
                Some(cmd) = self.dequeue.recv() => {
                    self.write.send(Message::from(cmd)).await.unwrap();
                }

                // If any data is avialable to be read, process it next.
                item = self.read.next() => {
                    if let Some(item) = item {
                        let data = item.unwrap().into_text().unwrap();
                        self.on_reply(&data, &mut account_notifications);
                    }
                }

                // Send a keepalive message.
                _ = interval.tick() => {
                    self.write.send(Message::from("{}")).await.unwrap();
                }
            }
        }
    }

    fn on_reply(
        &mut self,
        reply: &str,
        account_notifications: &mut UnboundedSender<AccountNotification>,
    ) {
        if let Ok(reply) = serde_json::from_str::<SubscriptionReply>(reply) {
            self.subscription_id_to_client_id
                .insert(reply.result, reply.id);
            self.client_id_to_subscription_id
                .insert(reply.id, reply.result);

            return;
        }

        if let Ok(mut reply) = serde_json::from_str::<AccountNotification>(reply) {
            if let Some(client_id) = self
                .subscription_id_to_client_id
                .get(&reply.params.subscription)
            {
                reply.params.subscription = *client_id;
                account_notifications.send(reply).unwrap();
            }
            return;
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SubscriptionReply {
    result: u64,
    id: u64,
}

struct Multiplexer {
    nodes: Vec<Node>,
    node_senders: Vec<UnboundedSender<std::string::String>>,
}

impl Multiplexer {
    pub async fn new(urls: &[Url]) -> Result<Self> {
        let mut nodes = Vec::new();
        let mut node_senders = Vec::new();

        for url in urls.iter() {
            let node = Node::new(url).await?;
            node_senders.push(node.get_channel());
            nodes.push(node);
        }

        Ok(Multiplexer {
            nodes,
            node_senders,
        })
    }

    pub fn send(&self, cmd: &str) -> Result<()> {
        for sender in self.node_senders.iter() {
            sender.send(cmd.to_string()).unwrap();
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        let (send_account_notification, mut receive_account_notification) = unbounded_channel();

        let mut tasks = Vec::new();
        for mut node in self.nodes.drain(0..) {
            let send_account_notification = send_account_notification.clone();
            tasks.push(tokio::spawn(async move {
                node.run(send_account_notification).await;
            }));
        }

        tasks.push(tokio::spawn(async move {
            let mut slot_by_subscription = HashMap::new();
            while let Some(result) = receive_account_notification.recv().await {
                if let Some(slot) = slot_by_subscription.get(&result.params.subscription) {
                    if result.params.result.context.slot > *slot {
                        println!("More recent slot: {}", result.params.result.context.slot);
                        // println!("{:?}", result);
                        slot_by_subscription.insert(
                            result.params.subscription,
                            result.params.result.context.slot,
                        );
                    } else {
                        println!("Stale slot: {}", *slot);
                    }
                } else {
                    println!("New slot: {}", result.params.result.context.slot);
                    // println!("{:?}", result);
                    slot_by_subscription.insert(
                        result.params.subscription,
                        result.params.result.context.slot,
                    );
                }
            }
        }));

        for task in tasks.iter_mut() {
            task.await.unwrap();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = vec![
        Url::parse("wss://api.mainnet-beta.solana.com:443").unwrap(),
        Url::parse("wss://solana-api.projectserum.com:443").unwrap(),
    ];
    let mut multiplexer = Multiplexer::new(&urls).await.unwrap();

    let cmd = r#"{
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                "5KKsLVU6TcbVDK4BS6K1DGDxnh4Q9xjYJ8XaDCG5t8ht",
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]
        }"#;

    multiplexer.send(cmd)?;
    multiplexer.run().await;

    Ok(())
}
