use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};
use std::str::FromStr;

/// Clients may specify the encoding. The server supports all except json.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub encoding: Encoding,
}

/// Each pubkey and commitment result in a new subscription.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {
    pub pubkey: String,
    pub commitment: Commitment,
}

/// Handles account subscriptions.
pub struct AccountSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl AccountSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

pub struct FormatState {
    bytes: Vec<u8>,
    result: jsonrpc::AccountNotificationResult,
}

impl SubscriptionHandler<Subscription, Metadata> for AccountSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "accountUnsubscribe"
    }

    /// Arbitrate websocket with HTTP.
    fn uses_pubsub() -> bool {
        true
    }

    /// Arbitrate websocket with HTTP.
    fn uses_http() -> bool {
        true
    }

    /// We always subscribe with base64 encoding and reformat to the client's
    /// desired encoding.
    fn format_http_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"getAccountInfo","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
            id.0,
            metadata.pubkey.as_str(),
            metadata.commitment.to_string()
        )
    }

    /// We always subscribe with base64 encoding and reformat to the client's
    /// desired encoding.
    fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"accountSubscribe","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
            id.0,
            metadata.pubkey.to_string(),
            metadata.commitment.to_string()
        )
    }

    /// Parses the user-specified pubkey and optional encoding and commitment.
    /// Encoding defaults to base64 and commitment defaults to finalized.
    fn parse_subscription(request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String> {
        let params = if let Some(serde_json::Value::Array(params)) = &request.params {
            Some(params)
        } else {
            None
        };
        if params.is_none() {
            return Err(String::from("missing array params"));
        }
        let params = params.unwrap();

        let pubkey = if let Some(serde_json::Value::String(pubkey)) = params.get(0) {
            Some(pubkey)
        } else {
            None
        };
        if pubkey.is_none() {
            return Err(String::from("missing pubkey"));
        }
        let pubkey = pubkey.unwrap();

        let mut subscription = Subscription {
            encoding: Encoding::Base64,
        };
        let mut metadata = Metadata {
            pubkey: pubkey.clone(),
            commitment: Commitment::Finalized,
        };

        if let Some(serde_json::Value::Object(options)) = params.get(1) {
            if let Some(serde_json::Value::String(encoding)) = options.get("encoding") {
                subscription.encoding = Encoding::from_str(&encoding)?
            };
            if let Some(serde_json::Value::String(commitment)) = options.get("commitment") {
                metadata.commitment = Commitment::from_str(&commitment)?
            }
        }
        Ok((subscription, metadata))
    }

    /// Transforms the base64-encoded output into the user's desired encoding.
    /// Caches the decoded base64 data and the partially-decoded account
    /// notification for reuse when formatting for multiple clients.
    fn format_notification(
        notification: &jsonrpc::Notification,
        subscription: &Subscription,
        state: &mut Option<FormatState>,
    ) -> Result<String, String> {
        if subscription.encoding == Encoding::Base64 {
            return serde_json::to_string(&notification)
                .map_err(|e| format!("unable to serialize to json: {}", e));
        }

        if state.is_none() {
            let result = serde_json::from_value::<jsonrpc::AccountNotificationResult>(
                notification.params.result.clone(),
            )
            .map_err(|e| format!("unable to parse notification: {}", e))?;

            let data_base64 = result.value.data.get(0).ok_or("no account data found")?;
            *state = Some(FormatState {
                bytes: base64::decode(data_base64)
                    .map_err(|e| format!("unable to decode base64 data: {}", e))?,
                result,
            });
        }

        let mut data = Vec::new();
        match subscription.encoding {
            Encoding::Base58 => {
                data.push(
                    bs58::encode(state.as_ref().ok_or("illegal state")?.bytes.as_slice())
                        .into_string(),
                );
                data.push("base58".to_string());
            }
            Encoding::Base64 => {
                unreachable!()
            }
            Encoding::Base64Zstd => {
                let compressed = zstd::stream::encode_all(
                    state.as_ref().ok_or("illegal state")?.bytes.as_slice(),
                    0,
                )
                .map_err(|e| format!("zlib error: {}", e))?;
                data.push(base64::encode(&compressed));
                data.push("base64+zstd".to_string());
            }
        }

        let mut result = state.as_ref().ok_or("illegal state")?.result.clone();
        result.value.data = data;
        let result =
            serde_json::to_string(&result).map_err(|e| format!("serialization error: {}", e))?;
        Ok(format!(
            r#"{{"jsonrpc":"2.0","method":"accountNotification","params":{{"subscription":{},"result":{}}}}}"#,
            notification.params.subscription, result
        ))
    }

    /// The HTTP endpoint overrides the returned notification with this method
    /// so that it can be processed consistently.
    fn poll_method() -> &'static str {
        "accountNotification"
    }

    /// Uniquify notifications by the returned data. The multiplexer will only
    /// broadcast notifications when the data has changed.
    fn notification_unique_key(notification: &jsonrpc::Notification) -> Option<String> {
        if let serde_json::Value::Object(result) = &notification.params.result {
            if let Some(serde_json::Value::Object(value)) = result.get("value") {
                if let Some(serde_json::Value::Array(data)) = value.get("data") {
                    if let Some(serde_json::Value::String(string)) = data.get(0) {
                        return Some(string.clone());
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json, Value};

    #[test]
    fn format_http_subscribe() {
        let metadata = Metadata {
            pubkey: String::from("hear ye hear ye"),
            commitment: Commitment::Confirmed,
        };
        let id = ServerInstructionID(42);

        let got = AccountSubscriptionHandler::format_http_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "getAccountInfo",
                "params": [
                    "hear ye hear ye",
                    {
                        "encoding": "base64",
                        "commitment": "confirmed",
                    }
                ]
            })
        );
    }

    #[test]
    fn format_pubsub_subscribe() {
        let metadata = Metadata {
            pubkey: String::from("hear ye hear ye"),
            commitment: Commitment::Confirmed,
        };
        let id = ServerInstructionID(42);

        let got = AccountSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "accountSubscribe",
                "params": [
                    "hear ye hear ye",
                    {
                        "encoding": "base64",
                        "commitment": "confirmed",
                    }
                ]
            })
        );
    }

    #[test]
    fn parse_subscription_fails_without_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: None,
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_non_array_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::String("what's up?".to_string())),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_missing_pubkey() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::Array(vec![])),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_non_string_pubkey() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::Array(vec![serde_json::Value::Bool(
                true,
            )])),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_with_pubkey() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["hello world"])),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(
            subscription,
            Subscription {
                encoding: Encoding::Base64,
            }
        );
        assert_eq!(
            metadata,
            Metadata {
                pubkey: "hello world".to_string(),
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_encoding() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["hello world", {"encoding": "base58"}])),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(
            subscription,
            Subscription {
                encoding: Encoding::Base58,
            }
        );
        assert_eq!(
            metadata,
            Metadata {
                pubkey: "hello world".to_string(),
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_commitment() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(
                serde_json::json!(["hello world", {"encoding": "base58", "commitment": "processed"}]),
            ),
        };
        let result = AccountSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(
            subscription,
            Subscription {
                encoding: Encoding::Base58,
            }
        );
        assert_eq!(
            metadata,
            Metadata {
                pubkey: "hello world".to_string(),
                commitment: Commitment::Processed,
            }
        );
    }

    #[test]
    fn format_notification_without_state() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "accountNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "data": [
                            base64::encode(b"google en passant"),
                            "base64",
                        ],
                        "executable": false,
                        "lamports": 987,
                        "owner": "blair_witch",
                        "rentEpoch": 999,
                    }
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base64,
        };

        let result = AccountSubscriptionHandler::format_notification(
            &notification,
            &subscription,
            &mut state,
        );
        assert!(state.is_none());
        assert!(result.is_ok());
        let result = result.unwrap();
        let result = from_str::<Value>(&result).unwrap();

        assert_eq!(
            result,
            json!({
                "jsonrpc": "2.0",
                "method": "accountNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "data": [
                                base64::encode(b"google en passant"),
                                "base64",
                            ],
                            "executable": false,
                            "lamports": 987,
                            "owner": "blair_witch",
                            "rentEpoch": 999,
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn format_notification_base58() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "accountNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "data": [
                            base64::encode(b"google en passant"),
                            "base64",
                        ],
                        "executable": false,
                        "lamports": 987,
                        "owner": "blair_witch",
                        "rentEpoch": 999,
                    }
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base58,
        };

        let result = AccountSubscriptionHandler::format_notification(
            &notification,
            &subscription,
            &mut state,
        );
        assert!(state.is_some());
        assert!(result.is_ok());
        let result = result.unwrap();
        let result = from_str::<Value>(&result).unwrap();

        assert_eq!(
            result,
            json!({
                "jsonrpc": "2.0",
                "method": "accountNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "data": [
                                bs58::encode(b"google en passant").into_string(),
                                "base58",
                            ],
                            "executable": false,
                            "lamports": 987,
                            "owner": "blair_witch",
                            "rentEpoch": 999,
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn format_notification_base64_zstd() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "accountNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "data": [
                            base64::encode(b"google en passant"),
                            "base64",
                        ],
                        "executable": false,
                        "lamports": 987,
                        "owner": "blair_witch",
                        "rentEpoch": 999,
                    }
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base64Zstd,
        };

        let result = AccountSubscriptionHandler::format_notification(
            &notification,
            &subscription,
            &mut state,
        );
        assert!(state.is_some());
        assert!(result.is_ok());
        let result = result.unwrap();
        let result = from_str::<Value>(&result).unwrap();

        let compressed = zstd::stream::encode_all(b"google en passant".as_slice(), 0).unwrap();

        assert_eq!(
            result,
            json!({
                "jsonrpc": "2.0",
                "method": "accountNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "data": [
                                base64::encode(compressed),
                                "base64+zstd",
                            ],
                            "executable": false,
                            "lamports": 987,
                            "owner": "blair_witch",
                            "rentEpoch": 999,
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn notification_unique_key() {
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "accountNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "data": [
                            base64::encode(b"google en passant"),
                            "base64",
                        ],
                        "executable": false,
                        "lamports": 987,
                        "owner": "blair_witch",
                        "rentEpoch": 999,
                    }
                }),
            },
        };
        assert_eq!(
            AccountSubscriptionHandler::notification_unique_key(&notification),
            Some(base64::encode(b"google en passant"))
        );
    }
}
