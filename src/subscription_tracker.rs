use crate::{channel_types::*, jsonrpc};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
};

pub trait ConstructFromClientID {
    fn from_client_id(id: &ClientID) -> Self;
}

pub struct SubscriptionTracker<S: Eq + Hash + ConstructFromClientID> {
    client_to_subscriptions: HashMap<ClientID, HashSet<ServerInstructionID>>,
    subscription_to_clients: HashMap<ServerInstructionID, HashSet<S>>,
    account_notifications: HashMap<ServerInstructionID, u64>,
}

impl<S: Eq + Hash + ConstructFromClientID> SubscriptionTracker<S> {
    pub fn new() -> Self {
        Self {
            client_to_subscriptions: HashMap::new(),
            subscription_to_clients: HashMap::new(),
            account_notifications: HashMap::new(),
        }
    }

    pub fn track_subscriber(
        &mut self,
        client: &ClientID,
        subscription: &ServerInstructionID,
        subscriber: S,
    ) {
        match self.client_to_subscriptions.entry(client.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(subscription.clone());
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(subscription.clone());
                entry.insert(set);
            }
        }

        match self.subscription_to_clients.entry(subscription.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(subscriber);
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(subscriber);
                entry.insert(set);
            }
        }
    }

    /// Returns true if the notification should be broadcasted. This is the case
    /// if the subscription is new or has a later slot than any existing notification.
    pub fn notification_is_most_recent(
        &mut self,
        notification: &jsonrpc::AccountNotification,
    ) -> bool {
        let latest = self
            .account_notifications
            .entry(ServerInstructionID(notification.params.subscription));
        match latest {
            Entry::Vacant(entry) => {
                entry.insert(notification.params.result.context.slot);
                true
            }
            Entry::Occupied(mut existing) => {
                if *existing.get() < notification.params.result.context.slot {
                    existing.insert(notification.params.result.context.slot);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn get_notification_subscribers(
        &self,
        notification: &jsonrpc::AccountNotification,
    ) -> Option<&HashSet<S>> {
        self.subscription_to_clients
            .get(&ServerInstructionID(notification.params.subscription))
    }

    /// Removes a single subscription for a client. Returns true if that
    /// subscription should be removed globally or false if it should remain.
    /// Returns None if the client wasn't subscribed.
    pub fn remove_single_subscription(
        &mut self,
        client: &ClientID,
        subscription: &ServerInstructionID,
    ) -> Option<bool> {
        Self::remove_single_subscription_from_map(
            &mut self.subscription_to_clients,
            client,
            subscription,
        )
    }

    fn remove_single_subscription_from_map(
        subscription_to_clients: &mut HashMap<ServerInstructionID, HashSet<S>>,
        client: &ClientID,
        subscription: &ServerInstructionID,
    ) -> Option<bool> {
        if let Entry::Occupied(mut entry) = subscription_to_clients.entry(subscription.clone()) {
            let subscribed_clients = entry.get_mut();
            if !subscribed_clients.remove(&S::from_client_id(client)) {
                return None;
            }
            if subscribed_clients.len() == 0 {
                entry.remove();

                // There aren't any subscribers remaining.
                return Some(true);
            } else {
                // There are still subscribers remaining.
                return Some(false);
            }
        } else {
            None
        }
    }

    /// Removes a client and returns all subscriptions that should be
    /// unsubscribed as a result.
    pub fn remove_client(&mut self, client: &ClientID) -> Vec<ServerInstructionID> {
        let mut to_unsubscribe = Vec::new();
        if let Some(subscriptions) = self.client_to_subscriptions.get(client) {
            for subscription in subscriptions.iter() {
                if let Some(should_remove) = Self::remove_single_subscription_from_map(
                    &mut self.subscription_to_clients,
                    client,
                    &subscription.clone(),
                ) {
                    if should_remove {
                        to_unsubscribe.push(subscription.clone());
                    }
                }
            }
        }
        self.client_to_subscriptions.remove(&client);
        to_unsubscribe
    }
}
