use crate::{channel_types::*, jsonrpc, subscriptions::tracker::SubscriptionTracker};
use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info};

/// Generic subscription handler. The Subscription type holds client-specific
/// parameters for how to materialize a notification. Metadata determines a
/// unique subscription. For example, the client's desired encoding is a
/// Subscription parameter, while the pubkey being subscribed to is part of the
/// subscription Metadata.
///
/// A single notification corresponding to one Metadata will thus be reformatted
/// potentially many times for each instance of Subscription. The FormatState
/// will be passed in for each instance of Subscription on each notification and
/// may be used to cache data useful for this re-serialization.
pub trait SubscriptionHandler<Subscription: Eq + Hash + Clone, Metadata: Eq + Hash + Clone> {
    /// Subscription-specific cache that is reused across Subscriptions for
    /// every new notification. For example, if each Subscription corresponds to
    /// a different encoding, FormatState may decode the base64 payload from the
    /// notification and store the raw bytes that will be encoded multiple
    /// times.
    type FormatState;

    /// Returns a subscription tracker.
    fn tracker_mut(&mut self) -> &mut SubscriptionTracker<Subscription, Metadata>;

    /// Returns the JSONRPC method to unsubscribe from a request.
    fn unsubscribe_method() -> &'static str;

    /// Returns the JSONRPC method to set on an HTTP poll response. This is only
    /// used if the subscription polls an HTTP endpoint.
    fn poll_method() -> &'static str;

    /// Returns true if this subscription should use PusSub endpoints.
    fn uses_pubsub() -> bool;

    /// Returns true if this subscription should use HTTP endpoints.
    fn uses_http() -> bool;

    /// Returns the raw JSONRPC call for an HTTP poll.
    fn format_http_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String;

    /// Returns the raw JSONRPC call for a PubSub subscribe.
    fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String;

    /// Parses the raw client subscription and returns the Subscription, which
    /// represents client-specific materialization parameters, and Metadata,
    /// which represents a unique subscription. For example, different encodings
    /// should correspond to the same underlying subscription, so encoding
    /// should belong in Subscription. Different subscribed pubkeys should
    /// correspond to different underlying subscriptions, so pubkey should be in
    /// Metadata.
    fn parse_subscription(request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String>;

    /// Returns a notion of timestamp for an input notification. Notifications
    /// that are missing timestamps will be dropped. Only the most recent
    /// notifications will be sent to clients. This function defaults to returning
    /// params.result.context.slot, but some notifications may require customization.
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

    /// Subscribes a client to this data. The subscription handler is
    /// responsible for parsing the request and forwarding appropriate messages
    /// to the underlying endpoints.
    fn subscribe(
        &mut self,
        client: ClientID,
        request: &jsonrpc::Request,
        next_instruction_id: &mut ServerInstructionID,
        send_to_client: UnboundedSender<ServerToClient>,
        pubsub_senders: &[UnboundedSender<ServerToPubsub>],
        http_senders: &[UnboundedSender<ServerToHTTP>],
    ) {
        match Self::parse_subscription(request) {
            Ok((subscription, metadata)) => {
                let (subscription_id, is_new) = self.tracker_mut().track_subscription(
                    &client,
                    next_instruction_id,
                    Some(subscription),
                    metadata.clone(),
                );

                info!(
                    "client {} subscribing to global id {}",
                    client.0, subscription_id.0
                );

                if is_new {
                    info!(
                        "initializing subscriptions for global id {}",
                        subscription_id.0
                    );
                    if Self::uses_pubsub() && pubsub_senders.len() > 0 {
                        let request = Self::format_pubsub_subscribe(&subscription_id, &metadata);
                        for endpoint in pubsub_senders.iter() {
                            endpoint
                                .send(ServerToPubsub::Subscribe {
                                    subscription: subscription_id.clone(),
                                    request: request.clone(),
                                })
                                .expect("pubsub endpoint channel died");
                        }
                    }
                    if Self::uses_http() && http_senders.len() > 0 {
                        let request = Self::format_http_subscribe(&subscription_id, &metadata);
                        for endpoint in http_senders.iter() {
                            endpoint
                                .send(ServerToHTTP::Subscribe {
                                    subscription: subscription_id.clone(),
                                    request: request.clone(),
                                    method: Self::poll_method().to_string(),
                                })
                                .expect("pubsub endpoint channel died");
                        }
                    }
                }

                send_subscription_response(send_to_client, request.id, subscription_id.0);
            }
            Err(err) => {
                send_error(
                    send_to_client,
                    jsonrpc::ErrorCode::InvalidRequest,
                    err.to_string(),
                    request.id,
                );
                return;
            }
        }
    }

    /// Unsubscribes a client from this data. The subscription handler is
    /// responsible for parsing the request and forwarding appropriate messages
    /// to the underlying endpoints if needed. Note that calling unsubscribe may
    /// not unsubscribe from the endpoint if other clients are still
    /// subscribing.
    fn unsubscribe(
        &mut self,
        client: ClientID,
        request: &jsonrpc::Request,
        next_instruction_id: &mut ServerInstructionID,
        send_to_client: UnboundedSender<ServerToClient>,
        pubsub_senders: &[UnboundedSender<ServerToPubsub>],
        http_senders: &[UnboundedSender<ServerToHTTP>],
    ) {
        let mut to_unsubscribe = None;
        if let Some(serde_json::Value::Array(params)) = &request.params {
            if params.len() == 1 {
                if let Some(serde_json::Value::Number(num)) = params.get(0) {
                    to_unsubscribe = num.as_i64();
                }
            }
        }

        if to_unsubscribe.is_none() {
            send_error(
                send_to_client.clone(),
                jsonrpc::ErrorCode::InvalidParams,
                "unable to parse unsubscribe parameters".to_string(),
                request.id,
            );
            return;
        }
        let to_unsubscribe = ServerInstructionID(to_unsubscribe.unwrap());

        if let Some(should_remove_globally) = self
            .tracker_mut()
            .remove_single_subscription(&client, &to_unsubscribe)
        {
            info!(
                "pubsub client {} unsubscribing to {}",
                client.0, to_unsubscribe.0
            );
            send_unsubscribe_response(send_to_client, request.id);
            if should_remove_globally {
                remove_global_subscription(
                    &to_unsubscribe,
                    next_instruction_id,
                    if Self::uses_pubsub() {
                        Some(pubsub_senders)
                    } else {
                        None
                    },
                    if Self::uses_http() {
                        Some(http_senders)
                    } else {
                        None
                    },
                    Self::unsubscribe_method(),
                );
            }
        } else {
            send_error(
                send_to_client,
                jsonrpc::ErrorCode::InvalidRequest,
                "not currently subscribed".to_string(),
                request.id,
            );
            return;
        }
    }

    /// Unsubscribes a client from all subscriptions managed by this handler.
    /// The subscription handler is responsible for parsing the request and
    /// forwarding appropriate messages to the underlying endpoints if needed.
    /// Note that calling unsubscribe may not unsubscribe from the endpoint if
    /// other clients are still subscribing. This method is commonly called on
    /// client disconnect.
    fn unsubscribe_client(
        &mut self,
        client: ClientID,
        next_instruction_id: &mut ServerInstructionID,
        pubsub_senders: &[UnboundedSender<ServerToPubsub>],
        http_senders: &[UnboundedSender<ServerToHTTP>],
    ) {
        let mut to_unsubscribe = self.tracker_mut().remove_client(&client);
        for unsubscribe in to_unsubscribe.drain(0..) {
            remove_global_subscription(
                &unsubscribe,
                next_instruction_id,
                if Self::uses_pubsub() {
                    Some(pubsub_senders)
                } else {
                    None
                },
                if Self::uses_http() {
                    Some(http_senders)
                } else {
                    None
                },
                Self::unsubscribe_method(),
            );
        }
    }

    /// Returns a unique key for the given notification. A notification will
    /// broadcast if it is more recent than anything previously broadcasted, and
    /// the unique key is None or different from the previously broadcasted
    /// unique key.
    fn notification_unique_key(_notification: &jsonrpc::Notification) -> Option<String> {
        None
    }

    /// Broadcasts a notification to all subscribed clients. The handler is
    /// responsible for transforming the notification into the raw JSONRPC
    /// replies that will be forwarded per each client's configured
    /// Subscription.
    fn broadcast(
        &mut self,
        notification: jsonrpc::Notification,
        send_to_client: &HashMap<ClientID, UnboundedSender<ServerToClient>>,
    ) {
        if let Some(timestamp) = Self::get_notification_timestamp(&notification) {
            let id = ServerInstructionID(notification.params.subscription);
            if self.tracker_mut().notification_should_broadcast(
                &id,
                timestamp,
                Self::notification_unique_key(&notification),
            ) {
                if let Some(subscriptions) = self.tracker_mut().get_notification_subscribers(&id) {
                    let mut cache: HashMap<Subscription, String> = HashMap::new();
                    let mut format_state = None;
                    for subscription in subscriptions.iter() {
                        if let Some(sender) = send_to_client.get(&subscription.client) {
                            if let Some(ref payload) = subscription.subscription {
                                let insert_payload = payload.clone();
                                match cache.entry(insert_payload) {
                                    Entry::Occupied(element) => {
                                        send_string(sender.clone(), element.get().clone());
                                    }
                                    Entry::Vacant(element) => {
                                        match Self::format_notification(
                                            &notification,
                                            &payload,
                                            &mut format_state,
                                        ) {
                                            Ok(value) => {
                                                send_string(sender.clone(), value.clone());
                                                element.insert(value);
                                            }
                                            Err(err) => {
                                                error!(
                                                    "notification could not be formatted: {}",
                                                    err
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Broadcasts a notification to all subscribed clients and then removes
    /// subscription tracking without issuing an unsubscribe request. This
    /// should be used for subscriptions that deliver only one notification.
    fn broadcast_and_unsubscribe(
        &mut self,
        notification: jsonrpc::Notification,
        send_to_client: &HashMap<ClientID, UnboundedSender<ServerToClient>>,
    ) {
        let id = ServerInstructionID(notification.params.subscription);
        self.broadcast(notification, send_to_client);

        if let Some(subscriptions) = self.tracker_mut().get_notification_subscribers(&id) {
            let mut clients = Vec::new();
            for subscription in subscriptions.iter() {
                clients.push(subscription.client.clone());
            }
            for client in clients.iter() {
                self.tracker_mut().remove_client(client);
            }
        }
    }

    /// Returns raw JSONRPC notification data, formatted with the Subscription
    /// parameters. The state is shared across calls for a given notification.
    /// For example, the state will be passed in for every different encoding
    /// that a notification must be formatted to.
    fn format_notification(
        notification: &jsonrpc::Notification,
        _subscription: &Subscription,
        _state: &mut Option<Self::FormatState>,
    ) -> Result<String, String> {
        let result = serde_json::to_string(notification)
            .map_err(|e| format!("serialization error: {}", e))?;
        Ok(result)
    }
}

/// Unsubscribes from all configured endpoints. For HTTP endpoints, this simply
/// kills the polling task. For PubSub endpoints, this sends an unsubscribe
/// message.
fn remove_global_subscription(
    unsubscribe: &ServerInstructionID,
    next_instruction_id: &mut ServerInstructionID,
    pubsub_senders: Option<&[UnboundedSender<ServerToPubsub>]>,
    http_senders: Option<&[UnboundedSender<ServerToHTTP>]>,
    unsubscribe_method: &str,
) {
    let instruction_id = next_instruction_id.clone();
    next_instruction_id.0 += 1;

    if let Some(send_to_http) = http_senders {
        for endpoint in send_to_http.iter() {
            if let Err(err) = endpoint.send(ServerToHTTP::Unsubscribe(unsubscribe.clone())) {
                error!("server to http channel failure: {}", err);
            }
        }
    }

    if let Some(send_to_pubsub) = pubsub_senders {
        for endpoint in send_to_pubsub.iter() {
            if let Err(err) = endpoint.send(ServerToPubsub::Unsubscribe {
                request_id: instruction_id.clone(),
                subscription: unsubscribe.clone(),
                method: unsubscribe_method.to_string(),
            }) {
                error!("server to pubsub channel failure: {}", err);
            }
        }
    }
}

/// Sends a string message to the client, logging on failure.
fn send_string(send_to_client: UnboundedSender<ServerToClient>, message: String) {
    if let Err(err) = send_to_client.send(ServerToClient::Message(message)) {
        error!("server to client channel failure: {}", err);
    }
}

/// Sends a subscription response to the client, logging on failure.
fn send_subscription_response(
    send_to_client: UnboundedSender<ServerToClient>,
    id: i64,
    result: i64,
) {
    let json = serde_json::to_string(&jsonrpc::SubscribeResponse {
        jsonrpc: "2.0".to_string(),
        result,
        id,
    })
    .expect("unable to serialize subscription reply to json");
    send_string(send_to_client, json);
}

/// Sends an unsubscribe response to the client, logging on failure.
fn send_unsubscribe_response(send_to_client: UnboundedSender<ServerToClient>, id: i64) {
    let json = serde_json::to_string(&jsonrpc::UnsubscribeResponse {
        jsonrpc: "2.0".to_string(),
        result: true,
        id,
    })
    .expect("unable to serialize unsubscribe reply to json");
    send_string(send_to_client, json);
}

/// Sends an error to the client, logging on failure.
fn send_error(
    send_to_client: UnboundedSender<ServerToClient>,
    code: jsonrpc::ErrorCode,
    message: String,
    id: i64,
) {
    let json = serde_json::to_string(&jsonrpc::Error {
        jsonrpc: "2.0".to_string(),
        code,
        message,
        id,
    })
    .expect("unable to serialize error to json");
    send_string(send_to_client, json);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json, Value};

    type DummySubscription = u16;
    type DummyMetadata = u32;

    struct DummyHandler {
        tracker: SubscriptionTracker<DummySubscription, DummyMetadata>,
    }

    impl SubscriptionHandler<DummySubscription, DummyMetadata> for DummyHandler {
        type FormatState = u64;

        fn tracker_mut(&mut self) -> &mut SubscriptionTracker<DummySubscription, DummyMetadata> {
            &mut self.tracker
        }

        fn unsubscribe_method() -> &'static str {
            "unsubscribe"
        }

        fn poll_method() -> &'static str {
            "poll"
        }

        fn uses_pubsub() -> bool {
            true
        }

        fn uses_http() -> bool {
            true
        }

        fn format_http_subscribe(id: &ServerInstructionID, metadata: &DummyMetadata) -> String {
            format!("http {} {}", id.0, metadata)
        }

        fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &DummyMetadata) -> String {
            format!("pubsub {} {}", id.0, metadata)
        }

        /// The params are [subscription, metadata].
        fn parse_subscription(
            request: &jsonrpc::Request,
        ) -> Result<(DummySubscription, DummyMetadata), String> {
            let mut subscription = 0;
            let mut metadata = 0;
            if let Some(serde_json::Value::Array(array)) = &request.params {
                if let Some(serde_json::Value::Number(sub)) = array.get(0) {
                    subscription = sub.as_u64().unwrap() as DummySubscription;
                }
                if let Some(serde_json::Value::Number(meta)) = array.get(1) {
                    metadata = meta.as_u64().unwrap() as DummyMetadata;
                }
            } else {
                return Err("error".to_string());
            }
            Ok((subscription, metadata))
        }

        fn format_notification(
            notification: &jsonrpc::Notification,
            subscription: &DummySubscription,
            state: &mut Option<Self::FormatState>,
        ) -> Result<String, String> {
            if let serde_json::Value::Object(result) = &notification.params.result {
                if let Some(serde_json::Value::Number(result)) = result.get("number") {
                    let num = result.as_f64().unwrap();
                    if state.is_none() {
                        *state = Some(0);
                    } else {
                        *state = Some(state.unwrap() + 1);
                    }
                    Ok((num as u64 + *subscription as u64).to_string())
                } else {
                    Err("error".to_string())
                }
            } else {
                Err("error".to_string())
            }
        }
    }

    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn subscribe_fails() {
        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: None,
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client) = unbounded_channel();
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "code": -32600,
                    "message": "error",
                    "id": 123,
                })
            );
        } else {
            panic!();
        }
    }

    #[tokio::test]
    async fn subscribe() {
        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: Some(json!([100, 200])),
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client) = unbounded_channel();
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 123,
                })
            );
        } else {
            panic!();
        }

        for mut receiver in http_receivers.drain(0..) {
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "http 1 200");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
        }

        for mut receiver in pubsub_receivers.drain(0..) {
            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "pubsub 1 200");
            } else {
                panic!();
            }
        }
    }

    #[tokio::test]
    async fn subscribe_twice_same_metadata() {
        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: Some(json!([100, 200])),
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client) = unbounded_channel();
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client.clone(),
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 123,
                })
            );
        } else {
            panic!();
        }

        let client = ClientID(1);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 456,
            method: "dummy".to_string(),
            params: Some(json!([300, 200])),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 456,
                })
            );
        } else {
            panic!();
        }

        for mut receiver in http_receivers.drain(0..) {
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "http 1 200");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
        }

        for mut receiver in pubsub_receivers.drain(0..) {
            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "pubsub 1 200");
            } else {
                panic!();
            }
        }
    }

    #[tokio::test]
    async fn subscribe_twice_different_metadata() {
        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: Some(json!([100, 200])),
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client) = unbounded_channel();
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client.clone(),
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 123,
                })
            );
        } else {
            panic!();
        }

        let client = ClientID(1);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 456,
            method: "dummy".to_string(),
            params: Some(json!([100, 300])),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 2,
                    "id": 456,
                })
            );
        } else {
            panic!();
        }

        for mut receiver in http_receivers.drain(0..) {
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "http 1 200");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(2));
                assert_eq!(request, "http 2 300");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
        }

        for mut receiver in pubsub_receivers.drain(0..) {
            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "pubsub 1 200");
            } else {
                panic!();
            }

            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(2));
                assert_eq!(request, "pubsub 2 300");
            } else {
                panic!();
            }
        }
    }

    #[tokio::test]
    async fn subscribe_twice_notify() {
        let mut send_to_client = HashMap::new();

        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: Some(json!([100, 200])),
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client_0) = unbounded_channel();
        send_to_client.insert(client.clone(), send_client.clone());
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client.clone(),
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client_0.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 123,
                })
            );
        } else {
            panic!();
        }

        let client = ClientID(1);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 456,
            method: "dummy".to_string(),
            params: Some(json!([300, 200])),
        };
        let (send_client, mut receive_client_1) = unbounded_channel();
        send_to_client.insert(client.clone(), send_client.clone());
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client_1.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 456,
                })
            );
        } else {
            panic!();
        }

        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "dummy".to_string(),
            params: jsonrpc::NotificationParams {
                result: json!({
                    "context": {
                        "slot": 1,
                    },
                    "number": 321,
                }),
                subscription: 1,
            },
        };

        handler.broadcast(notification, &send_to_client);

        for mut receiver in http_receivers.drain(0..) {
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "http 1 200");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
        }

        for mut receiver in pubsub_receivers.drain(0..) {
            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "pubsub 1 200");
            } else {
                panic!();
            }
        }

        if let Some(ServerToClient::Message(message)) = receive_client_0.recv().await {
            assert_eq!(from_str::<Value>(&message).unwrap(), json!(100 + 321),);
        } else {
            panic!();
        }

        if let Some(ServerToClient::Message(message)) = receive_client_1.recv().await {
            assert_eq!(from_str::<Value>(&message).unwrap(), json!(300 + 321),);
        } else {
            panic!();
        }
    }

    #[tokio::test]
    async fn unsubscribe_one_client() {
        let mut send_to_client = HashMap::new();

        let client = ClientID(0);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 123,
            method: "dummy".to_string(),
            params: Some(json!([100, 200])),
        };
        let mut next_instruction_id = ServerInstructionID(1);
        let (send_client, mut receive_client_0) = unbounded_channel();
        send_to_client.insert(client.clone(), send_client.clone());
        let mut http_senders = Vec::new();
        let mut http_receivers = Vec::new();
        let mut pubsub_senders = Vec::new();
        let mut pubsub_receivers = Vec::new();

        {
            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            http_senders.push(sender);
            http_receivers.push(receiver);
        }

        {
            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);

            let (sender, receiver) = unbounded_channel();
            pubsub_senders.push(sender);
            pubsub_receivers.push(receiver);
        }

        let mut handler = DummyHandler {
            tracker: SubscriptionTracker::new(),
        };
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client.clone(),
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client_0.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 123,
                })
            );
        } else {
            panic!();
        }

        let client = ClientID(1);
        let request = jsonrpc::Request {
            jsonrpc: "2.0".to_string(),
            id: 456,
            method: "dummy".to_string(),
            params: Some(json!([300, 200])),
        };
        let (send_client, mut receive_client_1) = unbounded_channel();
        send_to_client.insert(client.clone(), send_client.clone());
        handler.subscribe(
            client,
            &request,
            &mut next_instruction_id,
            send_client,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        if let Some(ServerToClient::Message(message)) = receive_client_1.recv().await {
            assert_eq!(
                from_str::<Value>(&message).unwrap(),
                json!({
                    "jsonrpc": "2.0",
                    "result": 1,
                    "id": 456,
                })
            );
        } else {
            panic!();
        }

        let notification = jsonrpc::Notification {
            jsonrpc: "2.0".to_string(),
            method: "dummy".to_string(),
            params: jsonrpc::NotificationParams {
                result: json!({
                    "context": {
                        "slot": 1,
                    },
                    "number": 321,
                }),
                subscription: 1,
            },
        };

        handler.unsubscribe_client(
            ClientID(1),
            &mut next_instruction_id,
            pubsub_senders.as_slice(),
            http_senders.as_slice(),
        );

        handler.broadcast(notification, &send_to_client);

        for mut receiver in http_receivers.drain(0..) {
            if let Some(ServerToHTTP::Subscribe {
                subscription,
                request,
                method,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "http 1 200");
                assert_eq!(method, "poll");
            } else {
                panic!();
            }
        }

        for mut receiver in pubsub_receivers.drain(0..) {
            if let Some(ServerToPubsub::Subscribe {
                subscription,
                request,
            }) = receiver.recv().await
            {
                assert_eq!(subscription, ServerInstructionID(1));
                assert_eq!(request, "pubsub 1 200");
            } else {
                panic!();
            }
        }

        if let Some(ServerToClient::Message(message)) = receive_client_0.recv().await {
            assert_eq!(from_str::<Value>(&message).unwrap(), json!(100 + 321),);
        } else {
            panic!();
        }
    }
}
