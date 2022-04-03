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
    latest_slot_by_subscription: HashMap<i64, i64>,
    client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
    subscriptions: Arc<Mutex<HashMap<i64, HashSet<u64>>>>,
}

impl EndpointArbitrator {
    pub fn new(
        receive_from_endpoints: UnboundedReceiver<EndpointToServer>,
        client_senders: Arc<Mutex<HashMap<u64, UnboundedSender<ServerToClient>>>>,
        subscriptions: Arc<Mutex<HashMap<i64, HashSet<u64>>>>,
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
        while self.run_once().await {}
    }

    async fn run_once(&mut self) -> bool {
        if let Some(from_endpoint) = self.receive_from_endpoints.recv().await {
            match from_endpoint {
                EndpointToServer::AccountNotification(notification) => {
                    self.on_notification(notification);
                }

                EndpointToServer::Error(error) => {
                    error!(received_error = format!("{:?}", error).as_str());
                }
            }
            true
        } else {
            false
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    fn setup() -> (
        UnboundedReceiver<ServerToClient>,
        UnboundedSender<EndpointToServer>,
        EndpointArbitrator,
    ) {
        let (send_to_server, receive_from_endpoints) = unbounded_channel();
        let mut client_senders = HashMap::new();
        let mut subscriptions = HashMap::new();
        let mut clients = HashSet::new();
        clients.insert(5);
        clients.insert(10);
        subscriptions.insert(123, clients);

        let (client_sender, client_receiver) = unbounded_channel();
        client_senders.insert(5, client_sender);

        let client_senders = Arc::new(Mutex::new(client_senders));
        let subscriptions = Arc::new(Mutex::new(subscriptions));
        let arbitrator = EndpointArbitrator::new(
            receive_from_endpoints,
            client_senders.clone(),
            subscriptions.clone(),
        );

        (client_receiver, send_to_server, arbitrator)
    }

    fn notification(slot: i64) -> EndpointToServer {
        EndpointToServer::AccountNotification(AccountNotification {
            jsonrpc: "2.0".to_string(),
            method: Method::accountNotification,
            params: NotificationParams {
                result: NotificationResult {
                    context: NotificationContext { slot },
                    value: serde_json::Value::Null,
                },
                subscription: 123,
            },
        })
    }

    #[tokio::test]
    async fn arbitration() {
        let (mut client_receiver, send_to_server, mut arbitrator) = setup();
        send_to_server.send(notification(100)).unwrap();
        arbitrator.run_once().await;

        // Should be ignored
        send_to_server.send(notification(100)).unwrap();
        arbitrator.run_once().await;

        // Should be ignored
        send_to_server.send(notification(99)).unwrap();
        arbitrator.run_once().await;

        send_to_server.send(notification(200)).unwrap();
        arbitrator.run_once().await;

        let message = client_receiver.recv().await.unwrap();
        if let ServerToClient::AccountNotification(message) = message {
            assert_eq!(message.jsonrpc, "2.0");
            assert_eq!(message.params.subscription, 123);
            assert_eq!(message.params.result.context.slot, 100);
        } else {
            panic!("unexpected return type");
        }

        let message = client_receiver.recv().await.unwrap();
        if let ServerToClient::AccountNotification(message) = message {
            assert_eq!(message.jsonrpc, "2.0");
            assert_eq!(message.params.subscription, 123);
            assert_eq!(message.params.result.context.slot, 200);
        } else {
            panic!("unexpected return type");
        }
    }
}
