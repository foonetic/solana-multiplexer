use crate::jsonrpc;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerInstructionID(pub i64);

/// Represents a unique HTTP request. For the HTTP API, the same connection may
/// issue multiple requests. Upgrading to the PubSub API will result in the
/// same request being reused for bidirectional websocket.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientID(pub u64);

#[derive(Debug)]
pub enum ClientToServer {
    Initialize(UnboundedSender<ServerToClient>),
    RemoveClient(ClientID),
    PubSubRequest {
        client: ClientID,
        request: jsonrpc::Request,
    },
    HTTPRequest {
        client: ClientID,
        request: jsonrpc::Request,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Encoding {
    Base58,
    Base64,
    Base64Zstd,
}

#[derive(Debug)]
pub enum ServerToClient {
    Initialize(ClientID),
    Message(String),
}

#[derive(Debug)]
pub enum ServerToHTTP {
    AccountSubscribe {
        subscription: ServerInstructionID,
        pubkey: String,
    },
    AccountUnsubscribe(ServerInstructionID),
    DirectRequest(jsonrpc::Request, UnboundedSender<ServerToClient>),
}

#[derive(Debug)]
pub enum ServerToPubsub {
    AccountSubscribe {
        subscription: ServerInstructionID,
        pubkey: String,
    },
    AccountUnsubscribe {
        request_id: ServerInstructionID,
        subscription: ServerInstructionID,
    },
}

#[derive(Debug)]
pub enum EndpointToServer {
    AccountNotification(jsonrpc::AccountNotification),
}
