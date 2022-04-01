use serde::{Deserialize, Serialize};

/// A websocket account notification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountNotification {
    pub jsonrpc: String,
    pub method: String,
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
#[derive(Serialize, Deserialize)]
pub struct SubscriptionReply {
    pub result: u64,
    pub id: u64,
}

/// Represents an instruction sent to an endpoint.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Instruction {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
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
