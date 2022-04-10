use crate::{channel_types::*, jsonrpc};
use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use hyper::{body::HttpBody, upgrade::Upgraded, Body, Request};
use hyper_tungstenite::{
    tungstenite::{protocol::CloseFrame, Message},
    HyperWebsocket, WebSocketStream,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

type Response = hyper::Response<Body>;
type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct ClientHandler {
    send_to_server: UnboundedSender<ClientToServer>,
}

impl ClientHandler {
    pub fn new(send_to_server: UnboundedSender<ClientToServer>) -> Self {
        ClientHandler { send_to_server }
    }

    fn on_upgrade(
        &mut self,
        request: Request<Body>,
        receive_from_server: UnboundedReceiver<ServerToClient>,
        client_id: ClientID,
    ) -> Result<Response, Error> {
        let (response, websocket) = hyper_tungstenite::upgrade(request, None)?;
        let mut client = PubSubClient::new(
            client_id,
            websocket,
            self.send_to_server.clone(),
            receive_from_server,
        );
        tokio::spawn(async move {
            client.run().await;
        });
        Ok(response)
    }

    async fn on_http(
        &mut self,
        request: Request<Body>,
        mut receive_from_server: UnboundedReceiver<ServerToClient>,
        client_id: ClientID,
    ) -> Result<Response, Error> {
        let (_parts, body) = request.into_parts();

        // Allow up to 10 MB in the request. This should allow all but the most
        // toxic queries.
        const MAX_ALLOWED_BODY: u64 = 1024 * 1024 * 10;

        let length = match body.size_hint().upper() {
            Some(v) => v,
            None => MAX_ALLOWED_BODY + 1,
        };

        if length < MAX_ALLOWED_BODY {
            let bytes = hyper::body::to_bytes(body).await;
            if let Err(err) = bytes {
                return Ok(Self::error_response(
                    jsonrpc::ErrorCode::InvalidRequest,
                    err.to_string(),
                    -1,
                ));
            }
            let bytes = bytes.unwrap();

            let request = serde_json::from_slice::<jsonrpc::Request>(bytes.as_ref());
            if let Err(err) = request {
                return Ok(Self::error_response(
                    jsonrpc::ErrorCode::InvalidRequest,
                    err.to_string(),
                    -1,
                ));
            }
            let request = request.unwrap();
            let request_id = request.id;

            if let Err(err) = self.send_to_server.send(ClientToServer::HTTPRequest {
                client: client_id.clone(),
                request,
            }) {
                return Ok(Self::error_response(
                    jsonrpc::ErrorCode::InternalError,
                    format!("channel failure: {}", err.to_string()),
                    request_id,
                ));
            }

            if let Some(message) = receive_from_server.recv().await {
                return Ok(match message {
                    ServerToClient::Initialize(_) => Self::error_response(
                        jsonrpc::ErrorCode::InternalError,
                        "unexpected initialization reply".to_string(),
                        request_id,
                    ),
                    ServerToClient::Message(message) => Response::new(Body::from(message)),
                });
            } else {
                return Ok(Self::error_response(
                    jsonrpc::ErrorCode::InternalError,
                    "no response".to_string(),
                    request_id,
                ));
            }
        } else {
            Ok(Self::error_response(
                jsonrpc::ErrorCode::InvalidRequest,
                "request too big".to_string(),
                -1,
            ))
        }
    }

    fn error_response(code: jsonrpc::ErrorCode, message: String, id: i64) -> Response {
        let obj = jsonrpc::Error {
            jsonrpc: "2.0".to_string(),
            code,
            message,
            id,
        };
        let json = serde_json::to_string(&obj).expect("unable to serialize known schema");
        Response::new(Body::from(json))
    }

    pub async fn run(&mut self, request: Request<Body>) -> Result<Response, Error> {
        let (send_to_client, mut receive_from_server) = unbounded_channel();
        if let Err(err) = self
            .send_to_server
            .send(ClientToServer::Initialize(send_to_client))
        {
            return Ok(Self::error_response(
                jsonrpc::ErrorCode::InternalError,
                err.to_string(),
                -1,
            ));
        }

        let client_id;
        if let Some(reply) = receive_from_server.recv().await {
            if let ServerToClient::Initialize(id) = reply {
                client_id = id;
            } else {
                return Ok(Self::error_response(
                    jsonrpc::ErrorCode::InternalError,
                    format!("unexpected message during initialization: {:?}", reply),
                    -1,
                ));
            }
        } else {
            return Ok(Self::error_response(
                jsonrpc::ErrorCode::InternalError,
                "no server reply during initialization".to_string(),
                -1,
            ));
        }

        if hyper_tungstenite::is_upgrade_request(&request) {
            self.on_upgrade(request, receive_from_server, client_id)
        } else {
            self.on_http(request, receive_from_server, client_id).await
        }
    }
}

struct PubSubClient {
    client_id: ClientID,
    websocket: Option<HyperWebsocket>,
    send_to_server: UnboundedSender<ClientToServer>,
    receive_from_server: UnboundedReceiver<ServerToClient>,
    enqueue_client: Option<UnboundedSender<Message>>,
}

impl PubSubClient {
    fn new(
        client_id: ClientID,
        websocket: HyperWebsocket,
        send_to_server: UnboundedSender<ClientToServer>,
        receive_from_server: UnboundedReceiver<ServerToClient>,
    ) -> Self {
        PubSubClient {
            client_id,
            websocket: Some(websocket),
            send_to_server,
            receive_from_server,
            enqueue_client: None,
        }
    }

    async fn run(&mut self) {
        let (enqueue_client, mut dequeue_client) = unbounded_channel();
        self.enqueue_client = Some(enqueue_client);
        let websocket = match self
            .websocket
            .take()
            .expect("run() called twice illegally")
            .await
        {
            Err(err) => {
                error!("failed to initialize pubsub client connection: {}", err);
                self.disconnect();
                return;
            }
            Ok(websocket) => websocket,
        };

        let (mut write, mut read) = websocket.split();
        tokio::spawn(async move {
            while let Some(message) = dequeue_client.recv().await {
                if let Err(err) = write.send(message).await {
                    error!("failed to enqueue client write: {}", err);
                }
            }
        });
        loop {
            if let Err(err) = self.poll(&mut read).await {
                error!("client pubsub poll failed: {}", err);
                self.disconnect();
                return;
            }
        }
    }

    async fn poll(
        &mut self,
        read: &mut SplitStream<WebSocketStream<Upgraded>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::select! {
            message = read.next() => {
                if let Some(message) = message {
                    self.on_client_message(message?)
                } else {
                    self.on_disconnect(None)
                }
            }

            Some(message) = self.receive_from_server.recv() => {
                self.on_server_message(message)
            }
        }
    }

    fn on_client_message(&mut self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            Message::Text(msg) => self.on_text_message(msg),
            Message::Binary(msg) => self.on_binary_message(msg),
            Message::Ping(_msg) => {
                // No need to send a reply: tungstenite takes care of this for you.
                Ok(())
            }
            Message::Pong(_msg) => {
                // Ignore: assume all pongs are valid.
                Ok(())
            }
            Message::Close(msg) => {
                // No need to send reply; this is handled by the library.
                self.on_disconnect(msg)
            }
            Message::Frame(_msg) => {
                unreachable!();
            }
        }
    }

    fn on_server_message(
        &mut self,
        message: ServerToClient,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            ServerToClient::Initialize(id) => {
                error!("client received unexpected initialization: {}", id.0);
                Ok(())
            }
            ServerToClient::Message(message) => self
                .enqueue_client
                .as_ref()
                .unwrap()
                .send(Message::Text(message))
                .map_err(|e| e.into()),
        }
    }

    fn on_text_message(&mut self, msg: String) -> Result<(), Box<dyn std::error::Error>> {
        let result = serde_json::from_str::<jsonrpc::Request>(&msg);
        match result {
            Err(err) => self.send_error(
                -1,
                jsonrpc::ErrorCode::InvalidRequest,
                format!("could not parse json request: {}", err),
            ),
            Ok(request) => self
                .send_to_server
                .send(ClientToServer::PubSubRequest {
                    client: self.client_id.clone(),
                    request,
                })
                .map_err(|e| e.into()),
        }
    }

    fn on_binary_message(&mut self, msg: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let result = serde_json::from_slice::<jsonrpc::Request>(&msg);
        match result {
            Err(err) => self.send_error(
                -1,
                jsonrpc::ErrorCode::InvalidRequest,
                format!("could not parse json request: {}", err),
            ),
            Ok(request) => self
                .send_to_server
                .send(ClientToServer::PubSubRequest {
                    client: self.client_id.clone(),
                    request,
                })
                .map_err(|e| e.into()),
        }
    }

    fn send_error(
        &mut self,
        id: i64,
        code: jsonrpc::ErrorCode,
        message: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let obj = jsonrpc::Error {
            jsonrpc: "2.0".to_string(),
            code,
            message,
            id,
        };
        let json = serde_json::to_string(&obj)?;
        self.enqueue_client
            .as_ref()
            .unwrap()
            .send(Message::Text(json))
            .map_err(|e| e.into())
    }

    fn disconnect(&mut self) {
        info!("disconnecting client {}", self.client_id.0);
        if let Err(err) = self
            .send_to_server
            .send(ClientToServer::RemoveClient(self.client_id.clone()))
        {
            error!("failed to disconnect client {}: {}", self.client_id.0, err);
        }
    }

    fn on_disconnect(&mut self, msg: Option<CloseFrame>) -> Result<(), Box<dyn std::error::Error>> {
        self.disconnect();
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            if let Some(msg) = msg {
                format!("closed with code {} and message {}", msg.code, msg.reason)
            } else {
                "closed with unknown reason".to_string()
            },
        )))
    }
}
