introduction
Getting Started

### Base URL

`wss://open-ws.coinglass.com/ws-api`

### Connection

To connect, use the following URL with your API key:

```
wss://open-ws.coinglass.com/ws-api?cg-api-key={your_api_key}
```

### Connection Handling

* The system will automatically disconnect if there are network issues.
* To maintain a stable connection, it is recommended to send a `'ping'` message every 20 seconds. Expect a `'pong'` response.

### Subscription

To subscribe to a channel, send the following message:

```json
{
    "method": "subscribe",
    "channels": ["liquidationOrders"]
}
```

### Unsubscription

To unsubscribe from a channel, send the following message:

```json
{
    "method": "unsubscribe",
    "channels": ["liquidationOrders"]
}
```

### Response Example

Upon receiving data, the response will look like this:

```json
{
    "channel": "liquidationOrders",
    "data": [
        {
            "baseAsset": "BTC",
            "exName": "Binance",
            "price": 56738.00,
            "side": 2,
            "symbol": "BTCUSDT",
            "time": 1725416318379,
            "volUsd": 3858.18400
        }
    ]
}
```

***

This format ensures clarity and ease of use when interacting with the WebSocket API.

Futures Trade Order

<span style={{ color: "red", fontSize: "24px", fontWeight: "bold" }}># Required Account Level: Standard Edition and Above</span>

```text
# Real-Time Futures Trade Orders Push

## Channel: `futures_trades@``{exchange}`_`{symbol}`@``{minVol}`

To subscribe to the `futures_trades` channel, send the following message: 




```

```json
{
    "method": "subscribe",
    "channels": ["futures_trades@Binance_BTCUSDT@10000"]
}
```

### Response Example

Upon receiving data, the response will look like this:

```json
{
    "channel": "futures_trades@Binance_BTCUSDT@10000",
    "data": [
        {
            "baseAsset": "BTC",
            "exName": "Binance",
            "price": 56738.00,
            "side": 2,//side=1  sell.   side=2   buy   
            "symbol": "BTCUSDT",
            "time": 1725416318379,
            "volUsd": 3858.18400
        }
    ]
}
```

<br />

### Response Example

Upon receiving data, the response will look like this: