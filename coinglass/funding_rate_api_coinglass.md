A. Funding Rate History (OHLC)

Endpoint: https://open-api-v4.coinglass.com/api/futures/funding-rate/history

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/option/exchange-oi-history?symbol=BTC&unit=USD&range=1h' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
1. exchange
string
required
Defaults to Binance
Futures exchange names (e.g., Binance, OKX) .Retrieve supported exchanges via the 'supported-exchange-pair' API.


2. symbol
string
required
Defaults to BTCUSDT
Trading pair (e.g., BTCUSDT). Retrieve supported pairs via the 'supported-exchange-pair' API.


3. interval
string
required
Defaults to 1d
Time interval for data aggregation. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

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
      "time": 1675987200000,
      "open": "0.01",
      "high": "0.01",
      "low": "0.007742",
      "close": "0.007742"
    },
    {
      "time": 1676073600000,
      "open": "0.007742",
      "high": "0.01",
      "low": "0.004176",
      "close": "0.005343"
    },
    {
      "time": 1676160000000,
      "open": "0.005343",
      "high": "0.01",
      "low": "0.000026",
      "close": "0.002098"
    }
  ]
}