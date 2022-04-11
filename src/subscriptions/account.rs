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
}
