# Solana Multiplexer

The multiplexer is a Solana PubSub server that manages subscriptions across
multiple HTTP and WebSocket endpoints. Sending a subscription message to the
multiplexer will forward the subscription request to all endpoints and arbitrate
the responses by slot number. The multiplexer may be useful for situations where
a client would like to access the lowest latency account data among a large pool
of RPC nodes.

The multiplexer currently supports all JSON HTTP RPC requests (i.e. including 
transactions) by forwarding round-robin to a single HTTP endpoint.

The following subset of PubSub subscriptions is supported:

- accountSubscribe
- logsSubscribe
- programSubscribe
- signatureSubscribe
- slotSubscribe
- rootSubscribe

In addition, as a special case, HTTP endpoints will poll accounts subscribed via
accountSubscribe at a specified frequency until the subscription is cancelled.
The HTTP and PubSub notifications are then arbitrated and returned via the
standard WebSocket notification format.

PubSub endpoints automatically issue subscription requests when needed and issue
unsubscribe requests when cancelled or when all subscribing clients disconnect.

Multiple subscribers may connect to the multiplexer simultaneously. Each
multiplexer subscriber may subscribe to a different set of accounts.
Subscription requests shared by multiple subscribers will be dispatched once by
the multiplexer and forwarded to each subscribed client.

## Example Invocation
The following invocation arbitrates among two websocket endpoints and two http
endpoints. The arbitrated websocket endpoint is served at `0.0.0.0:8900`. HTTP
endpoints are polled every 200 milliseconds.
```bash
multiplexer --endpoint wss://api.mainnet-beta.solana.com:443   \
            --endpoint https://api.mainnet-beta.solana.com:443 \
            --endpoint wss://solana-api.projectserum.com:443   \
            --endpoint https://solana-api.projectserum.com:443 \
            --listen_address 0.0.0.0:8900                      \
            --poll_frequency_milliseconds 200
```

## Metrics

The multiplexer publishes Prometheus metrics on the same address as the API
endpoint at path `/metrics`. For example:

```bash
$ curl localhost:8900/metrics
# HELP client_request_count number of client requests received
# TYPE client_request_count counter
client_request_count 15
# HELP latest_slot_seen Latest slot number seen across endpoints
# TYPE latest_slot_seen gauge
latest_slot_seen{endpoint="wss://api.devnet.solana.com/"} 127640127
# HELP metrics_serve_count number of times metrics have been served
# TYPE metrics_serve_count counter
metrics_serve_count 11
# HELP pubsub_message_count PubSub message count
# TYPE pubsub_message_count counter
pubsub_message_count{endpoint="wss://api.devnet.solana.com/",type="notification"} 44056
pubsub_message_count{endpoint="wss://api.devnet.solana.com/",type="pong"} 7
pubsub_message_count{endpoint="wss://api.devnet.solana.com/",type="subscribe"} 12
pubsub_message_count{endpoint="wss://api.devnet.solana.com/",type="unsubscribe"} 12
```

## Error handling

The multiplexer will attempt to reconnect broken websocket connections with an
exponential backoff. Upon successful connection, all existing notifications will
be automatically re-subscribed. 

Malformed messages will be silently dropped rather than forwarded to the client.
These will show up as pubsub messages of type error in the reported metrics.