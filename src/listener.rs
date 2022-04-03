use crate::messages::*;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use tracing::{error, info};

/// Listens to incoming WebSocket connections and spawns ClientManager instances
/// to manage the connection.
pub struct PortListener {
    next_client_id: u64,
    clients: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
    client_to_server: UnboundedSender<ClientToServer>,
}

impl PortListener {
    pub fn new(
        clients: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
        client_to_server: UnboundedSender<ClientToServer>,
    ) -> Self {
        Self {
            next_client_id: 0,
            clients,
            client_to_server,
        }
    }

    pub async fn run(&mut self, listener: TcpListener) {
        while let Ok((stream, _)) = listener.accept().await {
            let current_client_id = self.next_client_id;
            info!(newly_connected_client_id = current_client_id);
            let (send_to_client, receive_from_server) = unbounded_channel();
            {
                let mut clients = self.clients.lock().unwrap();
                clients.insert(current_client_id, send_to_client.clone());
            }
            self.next_client_id += 1;

            let mut client = ClientManager {
                id: current_client_id,
                send_to_server: self.client_to_server.clone(),
                send_to_client,
            };

            tokio::spawn(async move {
                client.run(stream, receive_from_server).await;
            });
        }
    }
}

/// Manages I/O between the WebSocket server and the client. This code must be
/// modified if our server must support additional instruction schemas.
struct ClientManager {
    id: u64,
    send_to_server: UnboundedSender<ClientToServer>,
    send_to_client: UnboundedSender<ServerToClient>,
}

impl ClientManager {
    pub async fn run(
        &mut self,
        tcp_stream: TcpStream,
        receive_from_server: UnboundedReceiver<ServerToClient>,
    ) {
        let stream = tokio_tungstenite::accept_async(tcp_stream).await;
        if !stream.is_ok() {
            error!(failed_connection_client_id = self.id);
            return;
        }
        let stream = stream.unwrap();
        let (send_to_client, mut receive_from_client) = stream.split();

        let mut sender = SendToClient {
            id: self.id,
            receive_from_server,
            send_to_client,
        };
        tokio::spawn(async move {
            sender.run().await;
        });

        while let Some(from_client) = receive_from_client.next().await {
            let data = from_client;
            if data.is_err() {
                if let Err(err) = self
                    .send_to_server
                    .send(ClientToServer::RemoveClient(self.id))
                {
                    error!(
                        client_id = self.id,
                        channel_send_failure = err.to_string().as_str()
                    );
                }
                return;
            }
            let data = data.unwrap().into_text();
            if data.is_err() {
                if let Err(err) = self
                    .send_to_server
                    .send(ClientToServer::RemoveClient(self.id))
                {
                    error!(
                        client_id = self.id,
                        channel_send_failure = err.to_string().as_str()
                    );
                }
                return;
            }
            let data = data.unwrap();

            if let Ok(instruction) = serde_json::from_str::<Instruction>(&data) {
                let message = ClientToServer::Instruction(self.id, instruction);
                if let Err(msg) = self.send_to_server.send(message) {
                    error!(
                        client_id = self.id,
                        channel_send_failure = msg.to_string().as_str()
                    );
                    return;
                }
            } else {
                let err = InvalidRequestError {
                    jsonrpc: "2.0".to_string(),
                    code: ErrorCode::InvalidRequest,
                    message: data.to_string(),
                    id: -1,
                };
                if let Err(err) = self
                    .send_to_client
                    .send(ServerToClient::InvalidRequestError(err))
                {
                    error!(
                        client_id = self.id,
                        channel_send_failure = err.to_string().as_str()
                    );
                }
            }
        }
    }
}

/// Forwards server notifications to the client. This must be changed if we want
/// to pass back additional message types.
struct SendToClient {
    id: u64,
    receive_from_server: UnboundedReceiver<ServerToClient>,
    send_to_client: SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl SendToClient {
    async fn run(&mut self) {
        while let Some(from_server) = self.receive_from_server.recv().await {
            let result = match from_server {
                ServerToClient::AccountNotification(msg) => {
                    let msg = serde_json::to_string(&msg).unwrap();
                    self.send_to_client.send(Message::from(msg)).await
                }
                ServerToClient::InvalidRequestError(msg) => {
                    let msg = serde_json::to_string(&msg).unwrap();
                    self.send_to_client.send(Message::from(msg)).await
                }
                ServerToClient::Error(msg) => {
                    let msg = serde_json::to_string(&msg).unwrap();
                    self.send_to_client.send(Message::from(msg)).await
                }
                ServerToClient::SubscriptionReply(msg) => {
                    let msg = serde_json::to_string(&msg).unwrap();
                    self.send_to_client.send(Message::from(msg)).await
                }
            };
            if let Err(err) = result {
                error!(
                    client_id = self.id,
                    websocket_send_failure = err.to_string().as_str()
                );
                // TODO: Return after retry
            }
        }
    }
}
