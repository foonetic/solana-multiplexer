# Solana Multiplexer

The multiplexer is a Solana PubSub server that manages account subscriptions across multiple HTTP and WebSocket endpoints. Sending an accountSubscribe message to the multiplexer will forward the subscription request to all endpoints and arbitrate the responses by slot number. The multiplexer may be useful for situations where a client would like to access the lowest latency account data among a large pool of RPC nodes.

The multiplexer currently only supports the accountSubscribe instruction. The multiplexer cancels subscriptions when all subscribing clients disconnect from the multiplexer. HTTP endpoints poll the subscribed account at a specified frequency until cancelled. PubSub endpoints issue subscription requests and issue unsubscribe requests when cancelled. The most recent account information retrieved across all HTTP and websocket endpoints is pushed to the user as an accountNotification response.

Multiple subscribers may connect to the multiplexer simultaneously. Each multiplexer subscriber may subscribe to a different set of accounts. Subscription requests shared by multiple subscribers will be dispatched once by the multiplexer and forwarded to each subscribed client.

## Example
The following invocation arbitrates among two websocket endpoints and two http endpoints. The arbitrated websocket endpoint is served at `0.0.0.0:8900`. HTTP endpoints are polled every 200 milliseconds.
```
multiplexer --endpoint wss://api.mainnet-beta.solana.com:443   \
            --endpoint https://api.mainnet-beta.solana.com:443 \
            --endpoint wss://solana-api.projectserum.com:443   \
            --endpoint https://solana-api.projectserum.com:443 \
            --listen_address 0.0.0.0:8900                      \
            --poll_frequency_milliseconds 200
```
