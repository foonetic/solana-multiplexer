use crate::{channel_types::*, jsonrpc, subscription_tracker};
use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info};

pub trait SubscriptionHandler<Subscription: Eq + Hash + Clone, Metadata: Eq + Hash + Clone> {
    type FormatState;

    fn tracker_mut(
        &mut self,
    ) -> &mut subscription_tracker::SubscriptionTracker<Subscription, Metadata>;

    fn unsubscribe_method() -> &'static str;
    fn poll_method() -> &'static str;
    fn uses_pubsub() -> bool;
    fn uses_http() -> bool;
    fn format_http_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String;
    fn format_pubsub_subscribe(id: &ServerInstructionID, metadata: &Metadata) -> String;
    fn parse_subscription(request: &jsonrpc::Request) -> Result<(Subscription, Metadata), String>;
    fn format_notification(
        notification: &jsonrpc::Notification,
        subscription: &Subscription,
        state: &mut Option<Self::FormatState>,
    ) -> Result<String, String>;
    fn transform_http_to_pubsub(
        result: jsonrpc::Notification,
    ) -> Result<jsonrpc::Notification, String>;

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
        if let serde_json::Value::Array(params) = &request.params {
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

    fn broadcast(
        &mut self,
        notification: jsonrpc::Notification,
        send_to_client: &HashMap<ClientID, UnboundedSender<ServerToClient>>,
    ) {
        if let Some(timestamp) = Self::get_notification_timestamp(&notification) {
            let id = ServerInstructionID(notification.params.subscription);
            if self
                .tracker_mut()
                .notification_is_most_recent(&id, timestamp)
            {
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
}

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

fn send_string(send_to_client: UnboundedSender<ServerToClient>, message: String) {
    if let Err(err) = send_to_client.send(ServerToClient::Message(message)) {
        error!("server to client channel failure: {}", err);
    }
}

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

fn send_unsubscribe_response(send_to_client: UnboundedSender<ServerToClient>, id: i64) {
    let json = serde_json::to_string(&jsonrpc::UnsubscribeResponse {
        jsonrpc: "2.0".to_string(),
        result: true,
        id,
    })
    .expect("unable to serialize unsubscribe reply to json");
    send_string(send_to_client, json);
}

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
