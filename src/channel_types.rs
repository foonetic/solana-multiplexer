use crate::jsonrpc;
use std::str::FromStr;
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

impl FromStr for Encoding {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "base58" => Ok(Encoding::Base58),
            "base64" => Ok(Encoding::Base64),
            "base64+zstd" => Ok(Encoding::Base64Zstd),
            _ => Err(s.to_string()),
        }
    }
}

impl ToString for Encoding {
    fn to_string(&self) -> String {
        match *self {
            Encoding::Base58 => "base58",
            Encoding::Base64 => "base64",
            Encoding::Base64Zstd => "base64+zstd",
        }
        .to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Commitment {
    Finalized,
    Confirmed,
    Processed,
}

impl FromStr for Commitment {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "finalized" => Ok(Commitment::Finalized),
            "confirmed" => Ok(Commitment::Confirmed),
            "processed" => Ok(Commitment::Processed),
            _ => Err(s.to_string()),
        }
    }
}

impl ToString for Commitment {
    fn to_string(&self) -> String {
        match *self {
            Commitment::Finalized => "finalized",
            Commitment::Confirmed => "confirmed",
            Commitment::Processed => "processed",
        }
        .to_string()
    }
}

#[derive(Debug)]
pub enum ServerToClient {
    Initialize(ClientID),
    Message(String),
}

#[derive(Debug)]
pub enum ServerToHTTP {
    Subscribe {
        subscription: ServerInstructionID,
        request: String,
        method: String,
    },
    Unsubscribe(ServerInstructionID),
    DirectRequest(jsonrpc::Request, UnboundedSender<ServerToClient>),
}

#[derive(Debug)]
pub enum ServerToPubsub {
    Subscribe {
        subscription: ServerInstructionID,
        request: String,
    },
    Unsubscribe {
        request_id: ServerInstructionID,
        subscription: ServerInstructionID,
        method: String,
    },
}

#[derive(Debug)]
pub enum EndpointToServer {
    Notification(jsonrpc::Notification),
}
