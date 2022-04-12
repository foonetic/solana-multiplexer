use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {
    transaction: String,
    commitment: Commitment,
}

pub struct SignatureSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl SignatureSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

pub struct FormatState {}

impl SubscriptionHandler<Subscription, Metadata> for SignatureSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "signatureUnsubscribe"
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
        let res = format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"signatureSubscribe","params":["{}",{{"commitment":"{}"}}]}}"#,
            id.0,
            metadata.transaction,
            metadata.commitment.to_string(),
        );
        res
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

        let transaction = if let Some(serde_json::Value::String(transaction)) = params.get(0) {
            transaction
        } else {
            return Err("missing transaction".to_string());
        }
        .clone();

        let mut metadata = Metadata {
            transaction,
            commitment: Commitment::Finalized,
        };

        if let Some(serde_json::Value::Object(object)) = params.get(1) {
            if let Some(serde_json::Value::String(commitment)) = object.get("commitment") {
                metadata.commitment = Commitment::from_str(commitment)
                    .map_err(|e| format!("unable to parse commitment: {}", e))?;
            }
        }

        Ok((Subscription {}, metadata))
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
            transaction: String::from("hear ye hear ye"),
            commitment: Commitment::Confirmed,
        };
        let id = ServerInstructionID(42);

        let got = SignatureSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "signatureSubscribe",
                "params": [
                    "hear ye hear ye",
                    {
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
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: None,
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_non_array_params() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::String("what's up?".to_string())),
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_missing_signature() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::Array(vec![])),
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_fails_with_non_string_signature() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::Value::Array(vec![serde_json::Value::Bool(
                true,
            )])),
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_with_signature() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["hello world"])),
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                transaction: "hello world".to_string(),
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_commitment() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "signatureSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["hello world", {"commitment": "processed"}])),
        };
        let result = SignatureSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                transaction: "hello world".to_string(),
                commitment: Commitment::Processed,
            }
        );
    }
}
