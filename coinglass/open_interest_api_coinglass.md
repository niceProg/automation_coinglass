# Coinglass Open Interest Integration


A. Exchange Open Interest History

Endpoint: https://open-api-v4.coinglass.com/api/option/exchange-oi-history

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/option/exchange-oi-history?symbol=BTC&unit=USD&range=1h' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
1. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC,ETH).


2. unit
string
required
Defaults to USD
Specify the unit for the returned data. Supported values depend on the symbol. If symbol is BTC, choose between USD or BTC. For ETH, choose between USD or ETH.


3. range
string
required
Defaults to 1h
Time range for the data. Supported values: 1h, 4h, 12h, all.

Response JSON Example:
{
  "code": "0",
  "data": {
    "time_list": [
      1762225200000,
      1762228800000,
      1762232400000,
      1762236000000,
      1762239600000,
      1762243200000,
      1762246800000,
      1762250400000,
      1762254000000,
    ]
  }
}

B. Aggregated Open Interest History

Endpoint: https://open-api-v4.coinglass.com/api/futures/open-interest/aggregated-history


cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/open-interest/aggregated-history?symbol=BTC&interval=1m&start_time=1762272780000&end_time=1762272900000' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
1. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC). Retrieve supported coins via the 'supported-coins' API.

2. interval
string
required
Defaults to 1d
Time interval for data aggregation. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

3. limit
int32
Number of results per request. Default: 1000, Maximum: 1000

4. start_time
int64
Start timestamp in milliseconds (e.g., 1641522717000).

5. end_time
int64
End timestamp in milliseconds (e.g., 1641522717000).

6. unit
string
Defaults to usd
Unit for the returned data, choose between 'usd' or 'coin'.

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "time": 1762272780000,
      "open": "67699534280",
      "high": "67816595276",
      "low": "67699534280",
      "close": "67816595276"
    },
    {
      "time": 1762272840000,
      "open": "67816595276",
      "high": "67816595276",
      "low": "67780963726",
      "close": "67780963726"
    }
  ]
}



C. Exchange List

Endpoint: https://open-api-v4.coinglass.com/api/futures/open-interest/exchange-list



cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/open-interest/exchange-list?symbol=BTC' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
1. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC).Retrieve supported coins via the 'supported-coins' API.

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "exchange": "All",
      "symbol": "BTC",
      "open_interest_usd": 68885666262.5056,
      "open_interest_quantity": 677019.6017,
      "open_interest_by_coin_margin": 9934173686.38,
      "open_interest_by_stable_coin_margin": 58951492576.13,
      "open_interest_quantity_by_coin_margin": 97424.931,
      "open_interest_quantity_by_stable_coin_margin": 579594.6707,
      "open_interest_change_percent_5m": 0.4,
      "open_interest_change_percent_15m": 0.06,
      "open_interest_change_percent_30m": 0.25,
      "open_interest_change_percent_1h": -0.4,
      "open_interest_change_percent_4h": 0.22,
      "open_interest_change_percent_24h": 2.11
    },
    {
      "exchange": "CME",
      "symbol": "BTC",
      "open_interest_usd": 14069281680.38,
      "open_interest_quantity": 138487.4,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 14069281680.38,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 138487.4,
      "open_interest_change_percent_5m": 0.37,
      "open_interest_change_percent_15m": -0.07,
      "open_interest_change_percent_30m": 0.29,
      "open_interest_change_percent_1h": -0.28,
      "open_interest_change_percent_4h": 2.31,
      "open_interest_change_percent_24h": 0.23
    },
    {
      "exchange": "Binance",
      "symbol": "BTC",
      "open_interest_usd": 12297660457.3009,
      "open_interest_quantity": 120787.503,
      "open_interest_by_coin_margin": 2819536800,
      "open_interest_by_stable_coin_margin": 9478123657.3009,
      "open_interest_quantity_by_coin_margin": 27660.91,
      "open_interest_quantity_by_stable_coin_margin": 93126.593,
      "open_interest_change_percent_5m": 0.31,
      "open_interest_change_percent_15m": 0.01,
      "open_interest_change_percent_30m": 0.1,
      "open_interest_change_percent_1h": -0.48,
      "open_interest_change_percent_4h": -1.36,
      "open_interest_change_percent_24h": 2.32
    },
    {
      "exchange": "Gate",
      "symbol": "BTC",
      "open_interest_usd": 7311266434.59712,
      "open_interest_quantity": 71932.9576,
      "open_interest_by_coin_margin": 26627936,
      "open_interest_by_stable_coin_margin": 7284638498.59712,
      "open_interest_quantity_by_coin_margin": 261.06,
      "open_interest_quantity_by_stable_coin_margin": 71671.8976,
      "open_interest_change_percent_5m": 0.18,
      "open_interest_change_percent_15m": -0.22,
      "open_interest_change_percent_30m": -0.49,
      "open_interest_change_percent_1h": -1.95,
      "open_interest_change_percent_4h": -4.28,
      "open_interest_change_percent_24h": 1.26
    },
    {
      "exchange": "Bybit",
      "symbol": "BTC",
      "open_interest_usd": 7182251018.8,
      "open_interest_quantity": 70601.038,
      "open_interest_by_coin_margin": 1461433004,
      "open_interest_by_stable_coin_margin": 5720818014.8,
      "open_interest_quantity_by_coin_margin": 14363,
      "open_interest_quantity_by_stable_coin_margin": 56238.038,
      "open_interest_change_percent_5m": 0.69,
      "open_interest_change_percent_15m": 0.3,
      "open_interest_change_percent_30m": 0.51,
      "open_interest_change_percent_1h": -0.27,
      "open_interest_change_percent_4h": -0.4,
      "open_interest_change_percent_24h": -2.63
    },
    {
      "exchange": "MEXC",
      "symbol": "BTC",
      "open_interest_usd": 4385416104.55268,
      "open_interest_quantity": 43082.2631,
      "open_interest_by_coin_margin": 76966500,
      "open_interest_by_stable_coin_margin": 4308449604.55268,
      "open_interest_quantity_by_coin_margin": 756.111,
      "open_interest_quantity_by_stable_coin_margin": 42326.1521,
      "open_interest_change_percent_5m": -0.03,
      "open_interest_change_percent_15m": 0.27,
      "open_interest_change_percent_30m": 0.62,
      "open_interest_change_percent_1h": -0.58,
      "open_interest_change_percent_4h": 7.61,
      "open_interest_change_percent_24h": 23.34
    },
    {
      "exchange": "HTX",
      "symbol": "BTC",
      "open_interest_usd": 3980713146.684,
      "open_interest_quantity": 39129.15875583785,
      "open_interest_by_coin_margin": 44287200,
      "open_interest_by_stable_coin_margin": 3936425946.684,
      "open_interest_quantity_by_coin_margin": 435.1187558378494,
      "open_interest_quantity_by_stable_coin_margin": 38694.04,
      "open_interest_change_percent_5m": 0.45,
      "open_interest_change_percent_15m": -0.02,
      "open_interest_change_percent_30m": 0.39,
      "open_interest_change_percent_1h": -0.24,
      "open_interest_change_percent_4h": -0.33,
      "open_interest_change_percent_24h": -2.45
    },
    {
      "exchange": "OKX",
      "symbol": "BTC",
      "open_interest_usd": 3627251808.889795,
      "open_interest_quantity": 35667.06088575852,
      "open_interest_by_coin_margin": 1146501800,
      "open_interest_by_stable_coin_margin": 2480750008.889795,
      "open_interest_quantity_by_coin_margin": 11271.84118575847,
      "open_interest_quantity_by_stable_coin_margin": 24395.219700000045,
      "open_interest_change_percent_5m": 0.47,
      "open_interest_change_percent_15m": 0.23,
      "open_interest_change_percent_30m": 0.32,
      "open_interest_change_percent_1h": -0.34,
      "open_interest_change_percent_4h": -0.06,
      "open_interest_change_percent_24h": 0.85
    },
    {
      "exchange": "Hyperliquid",
      "symbol": "BTC",
      "open_interest_usd": 2921754559.25672,
      "open_interest_quantity": 28733.67058,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 2921754559.25672,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 28733.67058,
      "open_interest_change_percent_5m": 0.44,
      "open_interest_change_percent_15m": 0.06,
      "open_interest_change_percent_30m": 0.64,
      "open_interest_change_percent_1h": -0.11,
      "open_interest_change_percent_4h": 1.93,
      "open_interest_change_percent_24h": -0.95
    },
    {
      "exchange": "Deribit",
      "symbol": "BTC",
      "open_interest_usd": 2771871600,
      "open_interest_quantity": 27090.06,
      "open_interest_by_coin_margin": 2771871600,
      "open_interest_by_stable_coin_margin": 0,
      "open_interest_quantity_by_coin_margin": 27090.06,
      "open_interest_quantity_by_stable_coin_margin": 0,
      "open_interest_change_percent_5m": 0.02,
      "open_interest_change_percent_15m": 0.07,
      "open_interest_change_percent_30m": 0.28,
      "open_interest_change_percent_1h": -0.03,
      "open_interest_change_percent_4h": -0.49,
      "open_interest_change_percent_24h": 9.54
    },
    {
      "exchange": "Bitget",
      "symbol": "BTC",
      "open_interest_usd": 2325434711.7966065,
      "open_interest_quantity": 22849.755576,
      "open_interest_by_coin_margin": 929948701.10858,
      "open_interest_by_stable_coin_margin": 1395486010.6880264,
      "open_interest_quantity_by_coin_margin": 9137.4524,
      "open_interest_quantity_by_stable_coin_margin": 13712.303176,
      "open_interest_change_percent_5m": 2.03,
      "open_interest_change_percent_15m": 0.53,
      "open_interest_change_percent_30m": 1.36,
      "open_interest_change_percent_1h": 0.8,
      "open_interest_change_percent_4h": 0.41,
      "open_interest_change_percent_24h": -0.93
    },
    {
      "exchange": "WhiteBIT",
      "symbol": "BTC",
      "open_interest_usd": 2028088503.0016,
      "open_interest_quantity": 19947.679,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 2028088503.0016,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 19947.679,
      "open_interest_change_percent_5m": 0.21,
      "open_interest_change_percent_15m": -0.09,
      "open_interest_change_percent_30m": -0.06,
      "open_interest_change_percent_1h": -0.88,
      "open_interest_change_percent_4h": -1.9,
      "open_interest_change_percent_24h": 4.23
    },
    {
      "exchange": "BingX",
      "symbol": "BTC",
      "open_interest_usd": 1923243375.49888,
      "open_interest_quantity": 18923.7002,
      "open_interest_by_coin_margin": 184001308.29888,
      "open_interest_by_stable_coin_margin": 1739242067.2,
      "open_interest_quantity_by_coin_margin": 1810.3024,
      "open_interest_quantity_by_stable_coin_margin": 17113.3978,
      "open_interest_change_percent_5m": -0.01,
      "open_interest_change_percent_15m": 0.09,
      "open_interest_change_percent_30m": -0.43,
      "open_interest_change_percent_1h": 0.78,
      "open_interest_change_percent_4h": 0.01,
      "open_interest_change_percent_24h": 6.71
    },
    {
      "exchange": "Crypto.com",
      "symbol": "BTC",
      "open_interest_usd": 1186328646.35484,
      "open_interest_quantity": 11539.9256,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 1186328646.35484,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 11539.9256,
      "open_interest_change_percent_5m": 0.45,
      "open_interest_change_percent_15m": 0.07,
      "open_interest_change_percent_30m": 0.27,
      "open_interest_change_percent_1h": -0.07,
      "open_interest_change_percent_4h": -0.19,
      "open_interest_change_percent_24h": -2.03
    },
    {
      "exchange": "Bitunix",
      "symbol": "BTC",
      "open_interest_usd": 712177893.30421,
      "open_interest_quantity": 7003.4831,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 712177893.30421,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 7003.4831,
      "open_interest_change_percent_5m": 0.71,
      "open_interest_change_percent_15m": 0.6,
      "open_interest_change_percent_30m": 0.99,
      "open_interest_change_percent_1h": 0.7,
      "open_interest_change_percent_4h": 5.21,
      "open_interest_change_percent_24h": 18.33
    },
    {
      "exchange": "Bitfinex",
      "symbol": "BTC",
      "open_interest_usd": 654163855.9958103,
      "open_interest_quantity": 6425.33990763,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 654163855.9958103,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 6425.33990763,
      "open_interest_change_percent_5m": 0.45,
      "open_interest_change_percent_15m": 0.1,
      "open_interest_change_percent_30m": 0.43,
      "open_interest_change_percent_1h": -0.18,
      "open_interest_change_percent_4h": -0.37,
      "open_interest_change_percent_24h": -2.8
    },
    {
      "exchange": "KuCoin",
      "symbol": "BTC",
      "open_interest_usd": 550418880.0426,
      "open_interest_quantity": 5410.947,
      "open_interest_by_coin_margin": 86636673,
      "open_interest_by_stable_coin_margin": 463782207.0426,
      "open_interest_quantity_by_coin_margin": 850.61,
      "open_interest_quantity_by_stable_coin_margin": 4560.337,
      "open_interest_change_percent_5m": 1.62,
      "open_interest_change_percent_15m": 0.82,
      "open_interest_change_percent_30m": 1.26,
      "open_interest_change_percent_1h": 1.15,
      "open_interest_change_percent_4h": 0.82,
      "open_interest_change_percent_24h": 2.65
    },
    {
      "exchange": "Bitmex",
      "symbol": "BTC",
      "open_interest_usd": 345648137.79935694,
      "open_interest_quantity": 3384.9813,
      "open_interest_by_coin_margin": 332228751.97295195,
      "open_interest_by_stable_coin_margin": 13419385.826405,
      "open_interest_quantity_by_coin_margin": 3253.13,
      "open_interest_quantity_by_stable_coin_margin": 131.8513,
      "open_interest_change_percent_5m": 0.37,
      "open_interest_change_percent_15m": 0.35,
      "open_interest_change_percent_30m": 0.3,
      "open_interest_change_percent_1h": 0.28,
      "open_interest_change_percent_4h": 0.67,
      "open_interest_change_percent_24h": -4.55
    },
    {
      "exchange": "Kraken",
      "symbol": "BTC",
      "open_interest_usd": 277642441.6725,
      "open_interest_quantity": 2730.3524,
      "open_interest_by_coin_margin": 29750097,
      "open_interest_by_stable_coin_margin": 247892344.6725,
      "open_interest_quantity_by_coin_margin": 295.79,
      "open_interest_quantity_by_stable_coin_margin": 2434.5624,
      "open_interest_change_percent_5m": 0.24,
      "open_interest_change_percent_15m": -0.16,
      "open_interest_change_percent_30m": -0.1,
      "open_interest_change_percent_1h": -0.5,
      "open_interest_change_percent_4h": 0.09,
      "open_interest_change_percent_24h": -4.18
    },
    {
      "exchange": "CoinEx",
      "symbol": "BTC",
      "open_interest_usd": 172668343.7315,
      "open_interest_quantity": 1695.9298,
      "open_interest_by_coin_margin": 24383315,
      "open_interest_by_stable_coin_margin": 148285028.7315,
      "open_interest_quantity_by_coin_margin": 239.5453,
      "open_interest_quantity_by_stable_coin_margin": 1456.3845,
      "open_interest_change_percent_5m": -0.09,
      "open_interest_change_percent_15m": -0.49,
      "open_interest_change_percent_30m": -0.36,
      "open_interest_change_percent_1h": 10.08,
      "open_interest_change_percent_4h": 13.51,
      "open_interest_change_percent_24h": 12.42
    },
    {
      "exchange": "Coinbase",
      "symbol": "BTC",
      "open_interest_usd": 128001049.6719,
      "open_interest_quantity": 1258.4235,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 128001049.6719,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 1258.4235,
      "open_interest_change_percent_5m": 1.12,
      "open_interest_change_percent_15m": -0.4,
      "open_interest_change_percent_30m": -0.3,
      "open_interest_change_percent_1h": -0.51,
      "open_interest_change_percent_4h": 1.11,
      "open_interest_change_percent_24h": -11.4
    },
    {
      "exchange": "dYdX",
      "symbol": "BTC",
      "open_interest_usd": 34383613.17463002,
      "open_interest_quantity": 337.9724,
      "open_interest_by_coin_margin": 0,
      "open_interest_by_stable_coin_margin": 34383613.17463002,
      "open_interest_quantity_by_coin_margin": 0,
      "open_interest_quantity_by_stable_coin_margin": 337.9724,
      "open_interest_change_percent_5m": 1.21,
      "open_interest_change_percent_15m": 0.78,
      "open_interest_change_percent_30m": 1.16,
      "open_interest_change_percent_1h": 0.38,
      "open_interest_change_percent_4h": -0.6,
      "open_interest_change_percent_24h": 16.7
    }
  ]
}



