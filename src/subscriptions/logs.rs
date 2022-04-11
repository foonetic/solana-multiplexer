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
    is_all: bool,
    is_all_with_votes: bool,
    sorted_mentions: Vec<String>,
    commitment: Commitment,
}

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
        let params = if let serde_json::Value::Array(params) = &request.params {
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

    fn format_notification(
        notification: &jsonrpc::Notification,
        _subscription: &Subscription,
        _state: &mut Option<FormatState>,
    ) -> Result<String, String> {
        let result = serde_json::to_string(notification)
            .map_err(|e| format!("serialization error: {}", e))?;
        Ok(result)
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
