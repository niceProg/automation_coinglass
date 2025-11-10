A. Price OHLC History

Endpoint: https://open-api-v4.coinglass.com/api/spot/price/history

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/spot/price/history?exchange=Binance&symbol=BTCUSDT&interval=1h&start_time=1758880800000&end_time=1758888000000' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
1. exchange
string
required
Defaults to Binance
spot exchange names (e.g., Binance, OKX) .Retrieve supported exchanges via the 'supported-exchange-pair' API.


2. symbol
string
required
Defaults to BTCUSDT
Trading pair (e.g., BTCUSDT). Retrieve supported pairs via the 'supported-exchange-pair' API.


3. interval
string
required
Defaults to 1h
Data aggregation time interval. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w.

4. start_time
string
Start timestamp in milliseconds (e.g., 1641522717000).

5. end_time
string
End timestamp in milliseconds (e.g., 1641522717000).

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "time": 1758880800000,
      "open": 109568.72,
      "high": 109568.72,
      "low": 108697.42,
      "close": 108895,
      "volume_usd": 80128331.5111
    },
    {
      "time": 1758884400000,
      "open": 108894.99,
      "high": 109200,
      "low": 108620.07,
      "close": 109133.5,
      "volume_usd": 100461566.185
    }
  ]
}