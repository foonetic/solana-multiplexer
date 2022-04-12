use crate::jsonrpc;
use std::str::FromStr;
use tokio::sync::mpsc::UnboundedSender;

/// Represents a globally unique server counter. The counter will be used for
/// instructions and subscription requests. Endpoints are responsible for
/// mapping any replies back to the globally unique identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerInstructionID(pub i64);

/// Represents a unique HTTP request. For the HTTP API, the same connection may
/// issue multiple requests. Upgrading to the PubSub API will result in the
/// same request being reused for bidirectional websocket.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientID(pub u64);

/// Messages passed from the client to the server.
#[derive(Debug)]
pub enum ClientToServer {
    /// Inform the server about this client and provide the channel to communicate with the client.
    Initialize(UnboundedSender<ServerToClient>),

    /// Inform the server that the client is disappearing.
    RemoveClient(ClientID),

    /// Inform the server that the client would like to issue a PubSub request.
    PubSubRequest {
        client: ClientID,
        request: jsonrpc::Request,
    },

    /// Inform the server that the client would like to issue an HTTP request.
    HTTPRequest {
        client: ClientID,
        request: jsonrpc::Request,
    },
}

/// Represents all supported encodings of account data. Note that JSON is
/// not supported.
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

/// Represents desired commitment levels.
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

/// Represents messages sent from the server to the client.
#[derive(Debug)]
pub enum ServerToClient {
    /// Server acknowledges the initialization and provides the client with an
    /// ID to be used. The client waits until the ID is received.
    Initialize(ClientID),

    /// Server sends a raw message to provide to the client.
    Message(String),
}

/// Represents communication from the server to an HTTP endpoint.
#[derive(Debug)]
pub enum ServerToHTTP {
    /// Server would like to subscribe. The subscription is performed by polling
    /// the given request and returning a WebSocket notification with the given
    /// method.
    Subscribe {
        subscription: ServerInstructionID,
        request: String,
        method: String,
    },

    /// Server would like to unsubscribe, which stops the polling.
    Unsubscribe(ServerInstructionID),

    /// Server would like to issue a single HTTP request on behalf of a client.
    /// The endpoint should forward the reply directly to the given client.
    DirectRequest(jsonrpc::Request, UnboundedSender<ServerToClient>),
}

/// Represents communication from the server to a PubSub endpoint.
#[derive(Debug)]
pub enum ServerToPubsub {
    /// Server would like to subscribe.
    Subscribe {
        subscription: ServerInstructionID,
        request: String,
    },

    /// Server would like to unsubscribe with the given method. Unsubscribing
    /// should use the unique request ID to avoid collision.
    Unsubscribe {
        request_id: ServerInstructionID,
        subscription: ServerInstructionID,
        method: String,
    },
}

/// Represents communication from an endpoint to the server.
#[derive(Debug)]
pub enum EndpointToServer {
    /// Raw notification response.
    Notification(jsonrpc::Notification, url::Url),
}
