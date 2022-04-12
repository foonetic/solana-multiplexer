use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {}

pub struct RootSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl RootSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

pub struct FormatState {}

impl SubscriptionHandler<Subscription, Metadata> for RootSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "rootUnsubscribe"
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

    fn format_pubsub_subscribe(id: &ServerInstructionID, _metadata: &Metadata) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"rootSubscribe"}}"#,
            id.0
        )
    }

    fn parse_subscription(_request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String> {
        Ok((Subscription {}, Metadata {}))
    }

    fn poll_method() -> &'static str {
        ""
    }

    /// The root returns only the slot as the result.
    fn get_notification_timestamp(notification: &jsonrpc::Notification) -> Option<u64> {
        if let serde_json::Value::Number(slot) = &notification.params.result {
            return slot.as_u64();
        }
        return None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json, Value};

    #[test]
    fn format_pubsub_subscribe() {
        let metadata = Metadata {};
        let id = ServerInstructionID(42);

        let got = RootSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "rootSubscribe",
            })
        );
    }

    #[test]
    fn get_notification_timestamp() {
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "rootNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!(999),
            },
        };
        assert_eq!(
            RootSubscriptionHandler::get_notification_timestamp(&notification),
            Some(999),
        );
    }
}
