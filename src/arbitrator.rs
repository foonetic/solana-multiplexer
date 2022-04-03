use crate::messages::*;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

/// Arbitrates between multiple endpoints, returning data from the latest
/// observed slot.
pub struct EndpointArbitrator {
    receive_from_endpoints: UnboundedReceiver<EndpointToServer>,
    latest_slot_by_subscription: HashMap<u64, u64>,
    client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
    subscriptions: Arc<Mutex<HashMap<u64, HashSet<u64>>>>,
}

impl EndpointArbitrator {
    pub fn new(
        receive_from_endpoints: UnboundedReceiver<EndpointToServer>,
        client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
        subscriptions: Arc<Mutex<HashMap<u64, HashSet<u64>>>>,
    ) -> Self {
        Self {
            receive_from_endpoints,
            latest_slot_by_subscription: HashMap::new(),
            client_senders,
            subscriptions,
        }
    }

    /// Listens to all endpoints. If the incoming data is an account
    /// notification, arbitrates and forwards to subscribing clients.
    pub async fn run(&mut self) {
        while let Some(from_endpoint) = self.receive_from_endpoints.recv().await {
            match from_endpoint {
                EndpointToServer::AccountNotification(notification) => {
                    self.on_notification(notification);
                }
            }
        }
    }

    fn on_notification(&mut self, notification: AccountNotification) {
        let slot = notification.params.result.context.slot;
        let should_send = match self
            .latest_slot_by_subscription
            .entry(notification.params.subscription)
        {
            Entry::Occupied(mut elem) => {
                if slot > *elem.get() {
                    *elem.get_mut() = slot;
                    true
                } else {
                    false
                }
            }

            Entry::Vacant(elem) => {
                info!(
                    subscription_id = notification.params.subscription,
                    started_at_slot = slot
                );
                elem.insert(slot);
                true
            }
        };

        if should_send {
            self.send(notification);
        }
    }

    /// Sends the notification to subscribers. The arbitrator assumes that the
    /// subscription id is consistent with the globally configured id sent by
    /// the server to each endpoint.
    fn send(&mut self, notification: AccountNotification) {
        let mut send_to = Vec::new();
        {
            let subscriptions = self.subscriptions.lock().unwrap();
            if let Some(got) = subscriptions.get(&notification.params.subscription) {
                let senders = self.client_senders.lock().unwrap();
                for client in got.iter() {
                    if let Some(sender) = senders.get(&client) {
                        send_to.push(sender.clone());
                    }
                }
            }
        }

        for sender in send_to.iter() {
            if let Err(err) = sender.send(ServerToClient::AccountNotification(notification.clone()))
            {
                // TODO: What should we do on channel failure?
                error!(arbitrator_channel_failure = err.to_string().as_str());
            }
        }
    }
}
