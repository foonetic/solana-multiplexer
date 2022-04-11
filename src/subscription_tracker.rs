use crate::channel_types::*;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::{Hash, Hasher},
};

#[derive(Debug, Eq)]
pub struct Subscription<S: Eq + Hash> {
    pub client: ClientID,
    pub subscription: Option<S>,
}
impl<S: Eq + Hash> Hash for Subscription<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.client.hash(state);
    }
}

impl<S: Eq + Hash> PartialEq for Subscription<S> {
    fn eq(&self, other: &Self) -> bool {
        self.client == other.client
    }
}

/// Tracks client subscriptions to data. The subscription type S contains data
/// related to how clients would like to view the data (for example, encoding).
/// Each client is restricted to a single subscription for a given underlying
/// data source. Underlying data sources are uniquified by metadata (for
/// example, pubkey and commitment).
pub struct SubscriptionTracker<S: Eq + Hash, M: Eq + Hash + Clone> {
    client_to_subscriptions: HashMap<ClientID, HashSet<ServerInstructionID>>,
    subscription_to_clients: HashMap<ServerInstructionID, HashSet<Subscription<S>>>,
    account_notifications: HashMap<ServerInstructionID, u64>,

    /// Subscriptions with the same metadata are considered equivalent, and will
    /// not result in a new subscription. For example, an account subscription
    /// is considered equal to another if the request is for the same public key
    /// and commitment level.
    metadata_to_subscription: HashMap<M, ServerInstructionID>,
    subscription_to_metadata: HashMap<ServerInstructionID, M>,
}

impl<S: Eq + Hash, M: Eq + Hash + Clone> SubscriptionTracker<S, M> {
    pub fn new() -> Self {
        Self {
            client_to_subscriptions: HashMap::new(),
            subscription_to_clients: HashMap::new(),
            account_notifications: HashMap::new(),
            metadata_to_subscription: HashMap::new(),
            subscription_to_metadata: HashMap::new(),
        }
    }

    /// Adds a subscriber, made unique by the metadata. Returns the ID and
    /// whether the subscription is new.
    pub fn track_subscription(
        &mut self,
        client: &ClientID,
        next_subscription: &mut ServerInstructionID,
        subscription: Option<S>,
        metadata: M,
    ) -> (ServerInstructionID, bool) {
        let (id, is_new) = match self.metadata_to_subscription.entry(metadata.clone()) {
            Entry::Occupied(existing) => (existing.get().clone(), false),
            Entry::Vacant(vacant) => {
                let id = next_subscription.clone();
                next_subscription.0 += 1;
                vacant.insert(id.clone());
                (id, true)
            }
        };
        if is_new {
            self.subscription_to_metadata.insert(id.clone(), metadata);
        }
        self.track_subscriber(client, &id, subscription);
        (id, is_new)
    }

    fn track_subscriber(
        &mut self,
        client: &ClientID,
        id: &ServerInstructionID,
        subscription: Option<S>,
    ) {
        match self.client_to_subscriptions.entry(client.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(id.clone());
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(id.clone());
                entry.insert(set);
            }
        }

        match self.subscription_to_clients.entry(id.clone()) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().insert(Subscription {
                    client: client.clone(),
                    subscription,
                });
            }
            Entry::Vacant(entry) => {
                let mut set = HashSet::new();
                set.insert(Subscription {
                    client: client.clone(),
                    subscription,
                });
                entry.insert(set);
            }
        }
    }

    /// Returns true if the notification should be broadcasted. This is the case
    /// if the subscription is new or has a later slot than any existing notification.
    pub fn notification_is_most_recent(
        &mut self,
        subscription: &ServerInstructionID,
        timestamp: u64,
    ) -> bool {
        let latest = self.account_notifications.entry(subscription.clone());
        match latest {
            Entry::Vacant(entry) => {
                entry.insert(timestamp);
                true
            }
            Entry::Occupied(mut existing) => {
                if *existing.get() < timestamp {
                    existing.insert(timestamp);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn get_notification_subscribers(
        &self,
        subscription: &ServerInstructionID,
    ) -> Option<&HashSet<Subscription<S>>> {
        self.subscription_to_clients.get(subscription)
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
            &mut self.metadata_to_subscription,
            &mut self.subscription_to_metadata,
        )
    }

    fn remove_single_subscription_from_map(
        subscription_to_clients: &mut HashMap<ServerInstructionID, HashSet<Subscription<S>>>,
        client: &ClientID,
        subscription: &ServerInstructionID,
        metadata_to_subscription: &mut HashMap<M, ServerInstructionID>,
        subscription_to_metadata: &mut HashMap<ServerInstructionID, M>,
    ) -> Option<bool> {
        if let Entry::Occupied(mut entry) = subscription_to_clients.entry(subscription.clone()) {
            let subscribed_clients = entry.get_mut();
            if !subscribed_clients.remove(&Subscription {
                client: client.clone(),
                subscription: None, // Arbitrary; not used in the has or equality.
            }) {
                return None;
            }
            if subscribed_clients.len() == 0 {
                entry.remove();

                Self::remove_metadata_from_map(
                    &subscription,
                    metadata_to_subscription,
                    subscription_to_metadata,
                );

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

    fn remove_metadata_from_map(
        subscription: &ServerInstructionID,
        metadata_to_subscription: &mut HashMap<M, ServerInstructionID>,
        subscription_to_metadata: &mut HashMap<ServerInstructionID, M>,
    ) {
        if let Entry::Occupied(metadata) = subscription_to_metadata.entry(subscription.clone()) {
            metadata_to_subscription.remove(&metadata.remove());
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
                    &subscription,
                    &mut self.metadata_to_subscription,
                    &mut self.subscription_to_metadata,
                ) {
                    if should_remove {
                        to_unsubscribe.push(subscription.clone());
                    }
                }
            }
        }
        self.client_to_subscriptions.remove(client);
        to_unsubscribe
    }
}
