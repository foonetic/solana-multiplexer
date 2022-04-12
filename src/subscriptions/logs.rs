use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};
use std::str::FromStr;

/// Clients may not configure anything about the response.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {}

/// Clients may pass "all"|"allWithVotes"|{"mentions":["<>"]}|{"commitment":<>}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {
    is_all: bool,
    is_all_with_votes: bool,
    sorted_mentions: Vec<String>,
    commitment: Commitment,
}

/// Handles log subscriptions.
pub struct LogsSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl LogsSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

/// No state is required for formatting.
pub struct FormatState {}

impl SubscriptionHandler<Subscription, Metadata> for LogsSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "logsUnsubscribe"
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
        let mut params = Vec::new();
        if metadata.is_all {
            params.push("\"all\"".to_string());
        }
        if metadata.is_all_with_votes {
            params.push("\"allWithVotes\"".to_string());
        }
        for mention in metadata.sorted_mentions.iter() {
            params.push(format!(r#"{{"mentions":["{}"]}}"#, mention));
        }
        params.push(format!(
            r#"{{"commitment":"{}"}}"#,
            metadata.commitment.to_string()
        ));

        let res = format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"logsSubscribe","params":[{}]}}"#,
            id.0,
            params.join(","),
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

        let mut metadata = Metadata {
            is_all: false,
            is_all_with_votes: false,
            sorted_mentions: Vec::new(),
            commitment: Commitment::Finalized,
        };

        for param in params.iter() {
            match param {
                serde_json::Value::Object(object) => {
                    if let Some(serde_json::Value::Array(mentions)) = object.get("mentions") {
                        if let Some(serde_json::Value::String(pubkey)) = mentions.get(0) {
                            metadata.sorted_mentions.push(pubkey.to_string());
                        }
                    } else if let Some(serde_json::Value::String(commitment)) =
                        object.get("commitment")
                    {
                        metadata.commitment = Commitment::from_str(commitment)
                            .map_err(|e| format!("unable to parse commitment: {}", e))?;
                    }
                }

                serde_json::Value::String(filter) => {
                    metadata.is_all |= filter == "all";
                    metadata.is_all_with_votes |= filter == "allWithVotes";
                }

                _ => {}
            }
        }

        metadata.sorted_mentions.sort_unstable();
        metadata.sorted_mentions.dedup();

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
            is_all: false,
            is_all_with_votes: false,
            sorted_mentions: vec![],
            commitment: Commitment::Processed,
        };
        let id = ServerInstructionID(42);

        let got = LogsSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "logsSubscribe",
                "params": [
                    {
                        "commitment": "processed",
                    }
                ]
            })
        );
    }

    #[test]
    fn format_pubsub_subscribe_is_all() {
        let metadata = Metadata {
            is_all: true,
            is_all_with_votes: false,
            sorted_mentions: vec![],
            commitment: Commitment::Confirmed,
        };
        let id = ServerInstructionID(42);

        let got = LogsSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "logsSubscribe",
                "params": [
                    "all",
                    {
                        "commitment": "confirmed",
                    }
                ]
            })
        );
    }

    #[test]
    fn format_pubsub_subscribe_is_all_with_votes() {
        let metadata = Metadata {
            is_all: false,
            is_all_with_votes: true,
            sorted_mentions: vec![],
            commitment: Commitment::Processed,
        };
        let id = ServerInstructionID(42);

        let got = LogsSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "logsSubscribe",
                "params": [
                    "allWithVotes",
                    {
                        "commitment": "processed",
                    }
                ]
            })
        );
    }

    #[test]
    fn format_pubsub_subscribe_mentions() {
        let metadata = Metadata {
            is_all: false,
            is_all_with_votes: false,
            sorted_mentions: vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
            ],
            commitment: Commitment::Processed,
        };
        let id = ServerInstructionID(42);

        let got = LogsSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": ["first"]},
                    {"mentions": ["second"]},
                    {"mentions": ["third"]},
                    {
                        "commitment": "processed",
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
        let result = LogsSubscriptionHandler::parse_subscription(&request);

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
        let result = LogsSubscriptionHandler::parse_subscription(&request);

        assert!(result.is_err());
    }

    #[test]
    fn parse_subscription_with_all() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["all"])),
        };
        let result = LogsSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                is_all: true,
                is_all_with_votes: false,
                sorted_mentions: Vec::new(),
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_all_with_votes() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!(["allWithVotes"])),
        };
        let result = LogsSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                is_all: false,
                is_all_with_votes: true,
                sorted_mentions: Vec::new(),
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_all_with_mentions() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!([
                {"mentions": ["ccc"]},
                {"mentions": ["aaa"]},
                {"mentions": ["bbb"]},
            ])),
        };
        let result = LogsSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                is_all: false,
                is_all_with_votes: false,
                sorted_mentions: vec!["aaa".to_string(), "bbb".to_string(), "ccc".to_string(),],
                commitment: Commitment::Finalized,
            }
        );
    }

    #[test]
    fn parse_subscription_with_all_with_commitment() {
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            method: "accountSubscribe".to_string(),
            id: 42,
            params: Some(serde_json::json!([
                {"commitment": "processed"},
            ])),
        };
        let result = LogsSubscriptionHandler::parse_subscription(&request);
        assert!(result.is_ok());
        let (subscription, metadata) = result.unwrap();
        assert_eq!(subscription, Subscription {});
        assert_eq!(
            metadata,
            Metadata {
                is_all: false,
                is_all_with_votes: false,
                sorted_mentions: Vec::new(),
                commitment: Commitment::Processed,
            }
        );
    }
}
