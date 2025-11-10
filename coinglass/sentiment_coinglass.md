A. Crypto Fear & Greed Index

Endpoint: https://open-api-v4.coinglass.com/api/index/fear-greed-history

cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/index/fear-greed-history \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
-

Response JSON Example:
{
  "code": "0",
  "data": {
    "data_list": [
      30,
      15,
      40,
      24,
      11,
      8,
      36,
      30,
      44,
      54,
      31,
      42,
      35,
      55,
      more...
  }
}

B. Funding Rate Exchange List

Endpoint: https://open-api-v4.coinglass.com/api/futures/funding-rate/exchange-list

cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/futures/funding-rate/exchange-list \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
-

Response Data:
{
  "code": "0",
  "msg": "success",
  "data": [
    {
      "symbol": "BTC", // Symbol
      "stablecoin_margin_list": [ // USDT/USD margin mode
        {
          "exchange": "Binance", // Exchange
          "funding_rate_interval": 8, // Funding rate interval (hours)
          "funding_rate": 0.007343, // Current funding rate
          "next_funding_time": 1745222400000 // Next funding time (milliseconds)
        },
        {
          "exchange": "OKX", // Exchange
          "funding_rate_interval": 8, // Funding rate interval (hours)
          "funding_rate": 0.00736901950628, // Current funding rate
          "next_funding_time": 1745222400000 // Next funding time (milliseconds)
        }
      ],
      "token_margin_list": [ // Coin-margined mode
        {
          "exchange": "Binance", // Exchange
          "funding_rate_interval": 8, // Funding rate interval (hours)
          "funding_rate": -0.001829, // Current funding rate
          "next_funding_time": 1745222400000 // Next funding time (milliseconds)
        }
      ]
    }
  ]
}

C. Hyperliquid Whale Alert

Endpoint: https://open-api-v4.coinglass.com/api/hyperliquid/whale-alert

cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/hyperliquid/whale-alert \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
-

Response Data Example:
{
  "code": "0",
  "msg": "success",
  "data": [
    {
      "user": "0x3fd4444154242720c0d0c61c74a240d90c127d33", // User address
      "symbol": "ETH",                                     // Symbol
      "position_size": 12700,                              // Position size (positive: long, negative: short)
      "entry_price": 1611.62,                              // Entry price
      "liq_price": 527.2521,                               // Liquidation price
      "position_value_usd": 21003260,                      // Position value (USD)
      "position_action": 2,                                // Position action type (1: open, 2: close)
      "create_time": 1745219517000                         // Entry time (timestamp in milliseconds)
    },
    {
      "user": "0x1cadadf0e884ac5527ae596a4fc1017a4ffd4e2c",
      "symbol": "BTC",
      "position_size": 33.54032,
      "entry_price": 87486.2,
      "liq_price": 44836.8126,
      "position_value_usd": 2936421.4757,
      "position_action": 2,
      "create_time": 1745219477000
    }
  ]
}

D. Whale Transfer
https://open-api-v4.coinglass.com/api/chain/whale-transfer

This endpoint provides large on-chain transfers (minimum $10M) within the past six months across major blockchains, including Bitcoin, Ethereum, Tron, Ripple, Dogecoin, Litecoin, Polygon, Algorand, Bitcoin Cash, and Solana.

cUrl example:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/chain/whale-transfer \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2'

Query Params: Use this as a filters or for requests, by symbol or date range

1. symbol
string
Trading coin (e.g., BTC, ETH, SOL, more). Retrieve supported coins via the 'supported-coins' API.

2. start_time
string
Start timestamp in milliseconds (e.g., 1641522717000).

3. end_time
string
End timestamp in milliseconds (e.g., 1641522717000).


Response samples:
{
  "code": "0",
  "data": [
    {
      "transaction_hash": "2a804d1a1543effd0f306375dd10bac80038b0783896ab3d001fa099b233c81a",
      "amount_usd": "10377682.185719607",
      "asset_quantity": "100",
      "asset_symbol": "BTC",
      "from": "unknown wallet",
      "to": "unknown wallet",
      "blockchain_name": "bitcoin",
      "block_height": 896229,
      "block_timestamp": 1746952313
    },
    {
      "transaction_hash": "fe0e3442309d2b14ed495db67e5b595c753fa496ae04925befe386ad79e87191",
      "amount_usd": "33001028.752833854",
      "asset_quantity": "317.99999424",
      "asset_symbol": "BTC",
      "from": "unknown wallet",
      "to": "unknown wallet",
      "blockchain_name": "bitcoin",
      "block_height": 896229,
      "block_timestamp": 1746952313
    },
    {
      "transaction_hash": "44e5c0a2b0b28863c8018f725f8cdcf12791f3b907de50e9a3e0f6e8ce96f62d",
      "amount_usd": "10595643.153306885",
      "asset_quantity": "102.1280795",
      "asset_symbol": "BTC",
      "from": "unknown wallet",
      "to": "Kraken",
      "blockchain_name": "bitcoin",
      "block_height": 896230,
      "block_timestamp": 1746952487
    },
    more..
    
