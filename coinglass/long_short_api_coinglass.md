A. Global Account Ratio 

Endpoint: https://open-api-v4.coinglass.com/api/futures/global-long-short-account-ratio/history


cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/global-long-short-account-ratio/history?exchange=Binance&symbol=BTCUSDT&interval=h1&start_time=1758798000000&end_time=1758805200000' \
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
Defaults to 1h
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
      "time": 1758798000000,
      "global_account_long_percent": 61.87,
      "global_account_short_percent": 38.13,
      "global_account_long_short_ratio": 1.62
    },
    {
      "time": 1758801600000,
      "global_account_long_percent": 61.87,
      "global_account_short_percent": 38.13,
      "global_account_long_short_ratio": 1.62
    }
  ]
}

B. Top Account Ratio History

Endpoint: https://open-api-v4.coinglass.com/api/futures/top-long-short-account-ratio/history


cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/top-long-short-account-ratio/history?exchange=Binance&symbol=BTCUSDT&interval=1h&start_time=1758801600000&end_time=1758808800000' \
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
Defaults to 1h
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
      "time": 1758801600000,
      "top_account_long_percent": 62.77,
      "top_account_short_percent": 37.23,
      "top_account_long_short_ratio": 1.69
    },
    {
      "time": 1758805200000,
      "top_account_long_percent": 63.42,
      "top_account_short_percent": 36.58,
      "top_account_long_short_ratio": 1.73
    }
  ]
}



