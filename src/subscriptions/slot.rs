use crate::{
    channel_types::*,
    jsonrpc,
    subscriptions::{handler::SubscriptionHandler, tracker::SubscriptionTracker},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata {}

pub struct SlotSubscriptionHandler {
    tracker: SubscriptionTracker<Subscription, Metadata>,
}

impl SlotSubscriptionHandler {
    pub fn new() -> Self {
        Self {
            tracker: SubscriptionTracker::new(),
        }
    }
}

pub struct FormatState {}

impl SubscriptionHandler<Subscription, Metadata> for SlotSubscriptionHandler {
    type FormatState = FormatState;

    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata> {
        &mut self.tracker
    }

    fn unsubscribe_method() -> &'static str {
        "slotUnsubscribe"
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
            r#"{{"jsonrpc":"2.0","id":{},"method":"slotSubscribe"}}"#,
            id.0
        )
    }

    fn parse_subscription(_request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String> {
        Ok((Subscription {}, Metadata {}))
    }

    fn poll_method() -> &'static str {
        ""
    }

    /// The slot notification returns the slot number directly in the result
    /// object instead of in a context.
    fn get_notification_timestamp(notification: &jsonrpc::Notification) -> Option<u64> {
        if let serde_json::Value::Object(result) = &notification.params.result {
            if let Some(serde_json::Value::Number(slot)) = result.get("slot") {
                return slot.as_u64();
            }
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

        let got = SlotSubscriptionHandler::format_pubsub_subscribe(&id, &metadata);
        assert_eq!(
            from_str::<Value>(&got).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": 42,
                "method": "slotSubscribe",
            })
        );
    }

    #[test]
    fn get_notification_timestamp() {
        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "slotNotification".to_string(),
            params: jsonrpc::NotificationParams {
                subscription: 42,
                result: json!({
                    "parent": 75,
                    "root": 44,
                    "slot": 76,
                }),
            },
        };
        assert_eq!(
            SlotSubscriptionHandler::get_notification_timestamp(&notification),
            Some(76),
        );
    }
}
