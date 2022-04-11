use crate::{channel_types::*, jsonrpc, subscription_handler, subscription_tracker};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub encoding: Encoding,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {
    pub pubkey: String,
    pub commitment: Commitment,
}

pub struct AccountSubscriptionHandler {
    tracker: subscription_tracker::SubscriptionTracker<Subscription, Metadata>,
}

impl AccountSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: subscription_tracker::SubscriptionTracker::new(),
        }
    }
}

pub struct FormatState {
    bytes: Vec<u8>,
    result: jsonrpc::AccountNotificationResult,
}

impl subscription_handler::SubscriptionHandler<Subscription, Metadata>
    for AccountSubscriptionHandler
{
    type FormatState = FormatState;

    fn tracker_mut(
        &mut self,
    ) -> &mut subscription_tracker::SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn notification_method() -> &'static str {
        "accountNotification"
    }

    fn unsubscribe_method() -> &'static str {
        "accountUnsubscribe"
    }

    fn uses_pubsub() -> bool {
        true
    }

    fn uses_http() -> bool {
        true
    }

    fn format_http_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"getAccountInfo","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
            id.0,
            metadata.pubkey.as_str(),
            metadata.commitment.to_string()
        )
    }

    fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"accountSubscribe","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
            id.0,
            metadata.pubkey.to_string(),
            metadata.commitment.to_string()
        )
    }

    fn subscription_type() -> SubscriptionType {
        SubscriptionType::Account
    }

    fn get_notification_timestamp(notification: &jsonrpc::Notification) -> Option<u64> {
        if let serde_json::Value::Object(result) = &notification.params.result {
            if let Some(serde_json::Value::Object(context)) = result.get("context") {
                if let Some(serde_json::Value::Number(slot)) = context.get("slot") {
                    return slot.as_u64();
                }
            }
        }
        return None;
    }

    fn parse_subscription(request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String> {
        let params = if let serde_json::Value::Array(params) = &request.params {
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

        let (encoding, commitment) = if let Some(serde_json::Value::Object(options)) = params.get(1)
        {
            let encoding =
                if let Some(serde_json::Value::String(encoding)) = options.get("encoding") {
                    Encoding::from_str(encoding)?
                } else {
                    Encoding::Base64
                };
            let commitment =
                if let Some(serde_json::Value::String(commitment)) = options.get("commitment") {
                    Commitment::from_str(commitment)?
                } else {
                    Commitment::Finalized
                };
            (encoding, commitment)
        } else {
            (Encoding::Base64, Commitment::Finalized)
        };

        Ok((
            Subscription { encoding },
            Metadata {
                pubkey: pubkey.clone(),
                commitment,
            },
        ))
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
}
