use serde::{Deserialize, Serialize};
use serde_repr::*;

/// JSONRPC error code, represented as an integer.
#[derive(Serialize_repr, Deserialize_repr, Debug, Clone)]
#[repr(i32)]
pub enum ErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
}

/// JSONRPC error message.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Error {
    pub jsonrpc: String,
    pub code: ErrorCode,
    pub message: String,
    pub id: i64,
}

/// JSONRPC request.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub jsonrpc: String,
    pub id: i64,
    pub method: String,
    pub params: Option<serde_json::Value>,
}

/// Generic JSONRPC HTTP response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub jsonrpc: String,
    pub id: i64,
    pub result: serde_json::Value,
}

/// Generic JSONRPC WebSocket notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: String,
    pub params: NotificationParams,
}

/// Generic JSONRPC WebSocket notification params.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationParams {
    pub result: serde_json::Value,
    pub subscription: i64,
}

/// Notification sent as an accountNotification response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountNotificationResult {
    pub context: ContextWithSlot,
    pub value: AccountNotificationValue,
}

/// Context sent as part of certain replies.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContextWithSlot {
    pub slot: u64,
}

/// Account data sent as part of accountNotification and programNotification
/// responses.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(non_snake_case)]
pub struct AccountNotificationValue {
    pub data: Vec<String>,
    pub executable: bool,
    pub lamports: u64,
    pub owner: String,
    pub rentEpoch: u64,
}

/// Subscription response received by the server from the endpoint, and sent by
/// the server to the client.
#[derive(Serialize, Deserialize, Debug)]
pub struct SubscribeResponse {
    pub jsonrpc: String,
    pub result: i64,
    pub id: i64,
}

/// Unsubscribe response received by the server from the endpoint, and sent by
/// the server to the client.
#[derive(Serialize, Deserialize, Debug)]
pub struct UnsubscribeResponse {
    pub jsonrpc: String,
    pub result: bool,
    pub id: i64,
}

/// Program data sent as part of programNotification responses.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgramNotificationResult {
    pub context: ContextWithSlot,
    pub value: ProgramNotificationValue,
}

/// Program data sent as part of programNotification responses.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgramNotificationValue {
    pub pubkey: String,
    pub account: AccountNotificationValue,
}
