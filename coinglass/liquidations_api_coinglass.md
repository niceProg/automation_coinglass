A. Coin Liquidation Heatmap Model3

Endpoint: https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-heatmap/model3


cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-heatmap/model3?symbol=BTC&range=3d' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params
1. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC). Retrieve supported coins via the 'supported-coins' API.

range
string
required
Defaults to 3d
Time range for data aggregation. Supported values: 12h, 24h, 3d, 7d, 30d, 90d, 180d, 1y.

Response JSON Example:
{
  "code": "0",
  "data": {
    "y_axis": [
      82277.9,
      82386.7,
      82495.5,
      82604.3,
      82713.1,
      82821.9,
      82930.7,
      83039.5,
      ],
      [
        506,
        147,
        414367
      ],
      [
        507,
        147,
        414367
      ],
      [
        508,
        147,
        414367
      ],
      [
        509,
        147,
        414367
      ],
      [
        510,
        147,
        414367
      ],
      more...


B. Coin Liquidation History

Endpoint: https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-history

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-history?exchange_list=Binance&symbol=BTC&interval=1d&start_time=1676073600000&end_time=1676332800000' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params
1. exchange_list
string
required
Defaults to Binance
List of exchange names to retrieve data from (e.g., 'Binance, OKX, Bybit')

2. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC). Retrieve supported coins via the 'supported-coins' API.

3. interval
string
required
Defaults to 1d
Time interval for data aggregation. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w.

4. start_time
int64
Start timestamp in milliseconds (e.g., 1641522717000).

5. end_time
int64
End timestamp in milliseconds (e.g., 1641522717000).

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "time": 1676073600000,
      "aggregated_long_liquidation_usd": 155384.67355,
      "aggregated_short_liquidation_usd": 932041.00963
    },
    {
      "time": 1676160000000,
      "aggregated_long_liquidation_usd": 2071978.02668,
      "aggregated_short_liquidation_usd": 1300893.83419
    },
    {
      "time": 1676246400000,
      "aggregated_long_liquidation_usd": 9246152.3683,
      "aggregated_short_liquidation_usd": 3859027.03582
    }
  ]
}

C. Real-Time Liquidation Orders Push

Channel: liquidationOrders
To subscribe to the liquidationOrders channel, send the following message:

JSON

{
    "method": "subscribe",
    "channels": ["liquidationOrders"]
}

Response Example
Upon receiving data, the response will look like this:

JSON

{
    "channel": "liquidationOrders",
    "data": [
        {
            "baseAsset": "BTC",
            "exName": "Binance",
            "price": 56738.00,
            "side": 2, //side=1   Long liquidation     side=2   Short liquidation
            "symbol": "BTCUSDT",
            "time": 1725416318379,
            "volUsd": 3858.18400
        }
    ]
}



      