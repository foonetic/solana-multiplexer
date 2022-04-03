use serde::{Deserialize, Serialize};
use serde_repr::*;

#[derive(Serialize_repr, Deserialize_repr, Debug, Clone)]
#[repr(i32)]
pub enum ErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Error {
    pub jsonrpc: String,
    pub code: ErrorCode,
    pub message: String,
    pub id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvalidRequestError {
    pub jsonrpc: String,
    pub code: ErrorCode,
    pub message: String,
    pub id: i8,
}

/// A websocket account notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountNotification {
    pub jsonrpc: String,
    pub method: Method,
    pub params: NotificationParams,
}

/// A synchronous account info result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountInfo {
    pub jsonrpc: String,
    pub result: NotificationResult,
    pub id: u64,
}

/// Internal parameters within an account notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationParams {
    pub result: NotificationResult,
    pub subscription: u64,
}

/// Internal result within an account params.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationResult {
    pub context: NotificationContext,
    pub value: serde_json::Value,
}

/// Internal context within an account result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationContext {
    pub slot: u64,
}

/// Represents a websocket subscription response.
#[derive(Serialize, Deserialize, Debug)]
pub struct SubscriptionReply {
    pub jsonrpc: String,
    pub result: u64,
    pub id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum Method {
    accountSubscribe,
    getAccountInfo,
    accountNotification,
}

/// Represents an instruction sent to an endpoint.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Instruction {
    pub jsonrpc: String,
    pub id: u64,
    pub method: Method,
    pub params: serde_json::Value,
}

impl Instruction {
    /// Returns the public key of the instruction. The public key is the first
    /// function parameter.
    pub fn get_pubkey(&self) -> Option<String> {
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

#[derive(Debug)]
pub enum ClientToServer {
    RemoveClient(u64),
    Instruction(u64, Instruction),
}

#[derive(Debug)]
pub enum ServerToClient {
    AccountNotification(AccountNotification),
    Error(Error),
    InvalidRequestError(InvalidRequestError),
    SubscriptionReply(SubscriptionReply),
}

#[derive(Debug)]
pub enum ServerToEndpoint {
    Instruction(Instruction),
}

#[derive(Debug)]
pub enum EndpointToServer {
    AccountNotification(AccountNotification),
}
