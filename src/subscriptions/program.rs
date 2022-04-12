use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};
use std::str::FromStr;

/// Clients may specify different encodings for account data.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub encoding: Encoding,
}

/// Clients may specify {"memcmp":{"offset":<>, "bytes":<>}}
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemCmp {
    pub offset: u64,
    pub bytes: String,
}

/// Clients must specify a pubkey and may optionally specify commitment, data
/// size filters, and memcmp filters. In practice, passing multiple data sizes
/// should return nothing, but we will replicate the query in case downstream
/// clients rely on some specific corner case of the API.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {
    pub pubkey: String,
    pub commitment: Commitment,
    pub data_size: Vec<u64>,
    pub memcmp: Vec<MemCmp>,
}

pub struct ProgramSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl ProgramSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

/// Holds the account data raw bytes and the deserialized notification to be
/// reused for different requested encodings.
pub struct FormatState {
    bytes: Vec<u8>,
    result: jsonrpc::ProgramNotificationResult,
}

impl SubscriptionHandler<Subscription, Metadata> for ProgramSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "programUnsubscribe"
    }

    fn uses_pubsub() -> bool {
        true
    }

    /// Note that we do NOT currently arbitrate with getProgramAccounts, since
    /// that call is relatively expensive.
    fn uses_http() -> bool {
        false
    }

    fn format_http_subscribe(_id: &ServerInstructionID, _metadata: &Metadata) -> String {
        String::new()
    }

    /// The subscription requests base64, which is re-encoded for each client
    /// into the client's requested encoding.
    fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String {
        let mut filters = Vec::new();
        for s in metadata.data_size.iter() {
            filters.push(format!(r#"{{"dataSize":{}}}"#, s));
        }
        for f in metadata.memcmp.iter() {
            filters.push(format!(
                r#"{{"memcmp":{{"offset":{},"bytes":"{}"}}}}"#,
                f.offset,
                f.bytes.clone()
            ));
        }

        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"programSubscribe","params":["{}",{{"encoding":"base64","commitment":"{}","filters":[{}]}}]}}"#,
            id.0,
            metadata.pubkey.to_string(),
            metadata.commitment.to_string(),
            filters.join(","),
        )
    }

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

        let mut metadata = Metadata {
            pubkey: pubkey.clone(),
            commitment: Commitment::Finalized,
            data_size: Vec::new(),
            memcmp: Vec::new(),
        };

        let mut subscription = Subscription {
            encoding: Encoding::Base64,
        };

        if let Some(serde_json::Value::Object(options)) = params.get(1) {
            if let Some(serde_json::Value::String(encoding)) = options.get("encoding") {
                subscription.encoding = Encoding::from_str(&encoding)
                    .map_err(|e| format!("unable to parse encoding: {}", e))?;
            }
            if let Some(serde_json::Value::String(commitment)) = options.get("commitment") {
                metadata.commitment = Commitment::from_str(&commitment)
                    .map_err(|e| format!("unable to parse commitment: {}", e))?;
            }

            if let Some(serde_json::Value::Array(filters)) = options.get("filters") {
                for filter in filters.iter() {
                    if let serde_json::Value::Object(filter) = filter {
                        if let Some(serde_json::Value::Number(size)) = filter.get("dataSize") {
                            metadata
                                .data_size
                                .push(size.as_u64().ok_or("invalid size detected".to_string())?);
                        }

                        if let Some(serde_json::Value::Object(memcmp)) = filter.get("memcmp") {
                            if let Some(serde_json::Value::Number(offset)) = memcmp.get("offset") {
                                if let Some(serde_json::Value::String(bytes)) = memcmp.get("bytes")
                                {
                                    metadata.memcmp.push(MemCmp {
                                        offset: offset
                                            .as_u64()
                                            .ok_or("invalid size detected".to_string())?,
                                        bytes: bytes.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        metadata.data_size.sort_unstable();
        metadata.data_size.dedup();

        metadata.memcmp.sort_unstable();
        metadata.memcmp.dedup();

        Ok((subscription, metadata))
    }

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
            let result = serde_json::from_value::<jsonrpc::ProgramNotificationResult>(
                notification.params.result.clone(),
            )
            .map_err(|e| format!("unable to parse notification: {}", e))?;

            let data_base64 = result
                .value
                .account
                .data
                .get(0)
                .ok_or("no account data found")?;
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
        result.value.account.data = data;
        let result =
            serde_json::to_string(&result).map_err(|e| format!("serialization error: {}", e))?;
        Ok(format!(
            r#"{{"jsonrpc":"2.0","method":"programNotification","params":{{"subscription":{},"result":{}}}}}"#,
            notification.params.subscription, result
        ))
    }

    fn poll_method() -> &'static str {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json, Value};

    #[test]
    fn format_pubsub_subscribe() {
        let metadata = Metadata {
            pubkey: String::from("hear ye hear ye"),
            commitment: Commitment::Confirmed,
            data_size: vec![3, 1, 2],
            memcmp: vec![
                MemCmp {
                    offset: 3,
                    bytes: "abc".to_string(),
                },
                MemCmp {
                    offset: 1,
                    bytes: "def".to_string(),
                },
                MemCmp {
                    offset: 2,
                    bytes: "ghi".to_string(),
                },
            ],
        };
        let id = ServerInstructionID(42);

        let got = ProgramSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "programSubscribe",
                "params": [
                    "hear ye hear ye",
                    {
                        "encoding": "base64",
                        "commitment": "confirmed",
                        "filters": [
                            {"dataSize": 3},
                            {"dataSize": 1},
                            {"dataSize": 2},
                            {"memcmp": {"offset": 3, "bytes": "abc"}},
                            {"memcmp": {"offset": 1, "bytes": "def"}},
                            {"memcmp": {"offset": 2, "bytes": "ghi"}},
                        ]
                    },
                ]
            })
        );
    }

    #[test]
    fn parse_subscription_fails_without_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "programSubscribe".to_string(),
            id: 42,
            params: None,
        };
        let result = ProgramSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_non_array_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "programSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::String("what's up?".to_string())),
        };
        let result = ProgramSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_missing_pubkey() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "programSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::Array(vec![])),
        };
        let result = ProgramSubscriptionHandler::parse_subscription(&request);

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
        let result = ProgramSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_with_pubkey() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "programSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["hello world"])),
        };
        let result = ProgramSubscriptionHandler::parse_subscription(&request);
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
                data_size: vec![],
                memcmp: vec![],
            }
        );
    }

    #[test]
    fn parse_subscription_with_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "programSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!([
                "hear ye hear ye",
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [
                        {"dataSize": 3},
                        {"dataSize": 1},
                        {"dataSize": 2},
                        {"memcmp": {"offset": 3, "bytes": "abc"}},
                        {"memcmp": {"offset": 1, "bytes": "def"}},
                        {"memcmp": {"offset": 2, "bytes": "ghi"}},
                    ]
                },
            ])),
        };
        let result = ProgramSubscriptionHandler::parse_subscription(&request);
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
                pubkey: "hear ye hear ye".to_string(),
                commitment: Commitment::Confirmed,
                data_size: vec![1, 2, 3],
                memcmp: vec![
                    MemCmp {
                        offset: 1,
                        bytes: "def".to_string(),
                    },
                    MemCmp {
                        offset: 2,
                        bytes: "ghi".to_string(),
                    },
                    MemCmp {
                        offset: 3,
                        bytes: "abc".to_string(),
                    }
                ],
            }
        );
    }

    #[test]
    fn format_notification_without_state() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "programNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "pubkey": "what is love",
                        "account": {
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
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base64,
        };

        let result = ProgramSubscriptionHandler::format_notification(
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
                "method": "programNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "pubkey": "what is love",
                            "account": {
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
                }
            })
        );
    }

    #[test]
    fn format_notification_base58() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "programNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "pubkey": "what is love",
                        "account": {
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
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base58,
        };

        let result = ProgramSubscriptionHandler::format_notification(
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
                "method": "programNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "pubkey": "what is love",
                            "account": {
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
                }
            })
        );
    }

    #[test]
    fn format_notification_base64_zstd() {
        let mut state = None;
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "programNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "context": {
                        "slot": 1234,
                    },
                    "value": {
                        "pubkey": "what is love",
                        "account": {
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
                }),
            },
        };

        let subscription = Subscription {
            encoding: Encoding::Base64Zstd,
        };

        let result = ProgramSubscriptionHandler::format_notification(
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
                "method": "programNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {
                            "slot": 1234,
                        },
                        "value": {
                            "pubkey": "what is love",
                            "account": {
                                "data": [
                                    base64::encode(zstd::encode_all(b"google en passant".as_slice(), 0).unwrap()),
                                    "base64+zstd",
                                ],
                                "executable": false,
                                "lamports": 987,
                                "owner": "blair_witch",
                                "rentEpoch": 999,
                            }
                        }
                    }
                }
            })
        );
    }
}
