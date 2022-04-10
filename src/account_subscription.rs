use crate::{channel_types::*, jsonrpc};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccountSubscription {
    pub encoding: Encoding,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccountSubscriptionMetadata {
    pub pubkey: String,
    pub commitment: Commitment,
}

pub fn parse_subscribe(
    request: &jsonrpc::Request,
) -> Result<(AccountSubscriptionMetadata, AccountSubscription), String> {
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

    let (encoding, commitment) = if let Some(serde_json::Value::Object(options)) = params.get(1) {
        let encoding = if let Some(serde_json::Value::String(encoding)) = options.get("encoding") {
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
        AccountSubscriptionMetadata {
            pubkey: pubkey.clone(),
            commitment,
        },
        AccountSubscription { encoding },
    ))
}

pub fn format_pubsub_subscription(
    id: &ServerInstructionID,
    metadata: &AccountSubscriptionMetadata,
) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{},"method":"accountSubscribe","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
        id.0,
        metadata.pubkey.to_string(),
        metadata.commitment.to_string()
    )
}

pub fn format_http_poll(
    id: &ServerInstructionID,
    metadata: &AccountSubscriptionMetadata,
) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{},"method":"getAccountInfo","params":["{}",{{"encoding":"base64","commitment":"{}"}}]}}"#,
        id.0,
        metadata.pubkey.to_string(),
        metadata.commitment.to_string()
    )
}

pub fn format_notification(
    bytes_cache: &mut Option<Vec<u8>>,
    notification: &jsonrpc::AccountNotification,
    subscription: &AccountSubscription,
) -> Option<String> {
    if subscription.encoding == Encoding::Base64 {
        return serde_json::to_string(notification).ok();
    }

    let data_base64 = notification.params.result.value.data.get(0)?;
    if bytes_cache.is_none() {
        *bytes_cache = Some(base64::decode(data_base64).ok()?);
    }

    let mut data = Vec::new();
    match subscription.encoding {
        Encoding::Base58 => {
            data.push(bs58::encode(bytes_cache.as_ref()?).into_string());
            data.push("base58".to_string());
        }
        Encoding::Base64 => {
            unreachable!()
        }
        Encoding::Base64Zstd => {
            let compressed = zstd::stream::encode_all(bytes_cache.as_ref()?.as_slice(), 0).ok()?;
            data.push(base64::encode(&compressed));
            data.push("base64+zstd".to_string());
        }
    }

    let mut notification = notification.clone();
    notification.params.result.value.data = data;

    serde_json::to_string(&notification).ok()
}
