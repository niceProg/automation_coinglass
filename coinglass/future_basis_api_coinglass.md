A. Futures Basis

Endpoint: https://open-api-v4.coinglass.com/api/futures/basis/history

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/basis/history?exchange=Binance&symbol=BTCUSDT&interval=1h&start_time=1758805200000&end_time=1758816000000' \
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
Trading pair (e.g., BTCUSDT). Retrieve supported pairs via the 'supported-exchange-pair' API. Likes (BTCUSDT, ETHUSDT, SOLUSDT, DOGEUSDT, XRPUSDT, ONDOUSDT)


3. interval
string
required
Defaults to 1h
Data aggregation time interval. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w.

4. start_time
int64
Start timestamp in milliseconds (e.g., 1641522717000).

5. end_time
int64
End timestamp in milliseconds (e.g., 1641522717000).

Response JSON Example:
{
  "code": "0",
  "msg": "success",
  "data": [
    {
      "time": 1758805200000,
      "open_basis": 0.028,
      "close_basis": 0.0374,
      "open_change": 31.11,
      "close_change": 41.5
    },
    {
      "time": 1758808800000,
      "open_basis": 0.0375,
      "close_basis": 0.0511,
      "open_change": 41.6,
      "close_change": 57
    },
    {
      "time": 1758812400000,
      "open_basis": 0.0511,
      "close_basis": 0.038,
      "open_change": 57.01,
      "close_change": 42.37
    }
  ],
  "success": true
}