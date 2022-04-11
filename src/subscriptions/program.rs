use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub encoding: Encoding,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemCmp {
    pub offset: u64,
    pub bytes: String,
}

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

    fn uses_http() -> bool {
        false
    }

    fn format_http_subscribe(_id: &ServerInstructionID, _metadata: &Metadata) -> String {
        String::new()
    }

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
            r#"{{"jsonrpc":"2.0","method":"accountNotification","params":{{"subscription":{},"result":{}}}}}"#,
            notification.params.subscription, result
        ))
    }

    fn poll_method() -> &'static str {
        ""
    }

    fn transform_http_to_pubsub(
        result: jsonrpc::Notification,
    ) -> Result<jsonrpc::Notification, String> {
        Ok(result)
    }
}
