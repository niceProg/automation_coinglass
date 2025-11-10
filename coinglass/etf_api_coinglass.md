A. Bitcoin ETF List

Endpoint: https://open-api-v4.coinglass.com/api/etf/bitcoin/list

cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/etf/bitcoin/list \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params:
- 

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "ticker": "GBTC",
      "fund_name": "Grayscale Bitcoin Trust ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "cik_code": "0001588489",
      "fund_type": "Spot",
      "market_cap_usd": "17549720040.0",
      "list_date": 1424822400000,
      "shares_outstanding": "218280100",
      "aum_usd": "17756923324.37",
      "management_fee_percent": "1.5",
      "last_trade_time": 1762439694932,
      "last_quote_time": 1762439694830,
      "volume_quantity": 80342,
      "volume_usd": 6451462.6,
      "price_usd": 80.4,
      "price_change_usd": -0.95,
      "price_change_percent": -1.17,
      "asset_details": {
        "net_asset_value_usd": 17756923324.37,
        "premium_discount_percent": 0,
        "holding_quantity": 170954.0797,
        "change_percent_24h": -0.29,
        "change_quantity_24h": -492.6215,
        "change_percent_7d": -0.33,
        "change_quantity_7d": -559.1846,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439400000
    },
    {
      "ticker": "BITO",
      "fund_name": "ProShares Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "cik_code": "0001174610",
      "fund_type": "Futures",
      "market_cap_usd": "1750679121.6306",
      "list_date": 1634601600000,
      "shares_outstanding": "108728379",
      "aum_usd": "2762896847.00",
      "last_trade_time": 1762439694563,
      "last_quote_time": 1762439692099,
      "volume_quantity": 861096,
      "volume_usd": 13846337.5704,
      "price_usd": 16.1014,
      "price_change_usd": -0.1886,
      "price_change_percent": -1.16,
      "asset_details": {
        "net_asset_value_usd": 2762896847,
        "premium_discount_percent": 0.06,
        "holding_quantity": 29922.2388,
        "update_date": "2024-11-20"
      },
      "update_timestamp": 1762439401000
    },
    {
      "ticker": "IBIT",
      "fund_name": "iShares Bitcoin Trust",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "XNAS",
      "cik_code": "0001980994",
      "fund_type": "Spot",
      "market_cap_usd": "82102430000.00",
      "list_date": 1704931200000,
      "shares_outstanding": "1409000000",
      "aum_usd": "82756896982.39",
      "management_fee_percent": "0.25",
      "last_trade_time": 1762439694830,
      "last_quote_time": 1762439694827,
      "volume_quantity": 3979175,
      "volume_usd": 231548193.25,
      "price_usd": 58.27,
      "price_change_usd": -0.65,
      "price_change_percent": -1.1,
      "asset_details": {
        "net_asset_value_usd": 83186003973,
        "premium_discount_percent": -0.2,
        "holding_quantity": 796092.2007,
        "change_percent_24h": -0.45,
        "change_quantity_24h": -3608.828,
        "change_percent_7d": -1.17,
        "change_quantity_7d": -9442.2829,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439401000
    },
    {
      "ticker": "FBTC",
      "fund_name": "Fidelity Wise Origin Bitcoin Fund",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001852317",
      "fund_type": "Spot",
      "market_cap_usd": "20310953500.000",
      "list_date": 1704931200000,
      "shares_outstanding": "226900000",
      "aum_usd": "21353112007.00",
      "management_fee_percent": "0.25",
      "last_trade_time": 1762439694780,
      "last_quote_time": 1762439694241,
      "volume_quantity": 178131,
      "volume_usd": 15928474.02,
      "price_usd": 89.515,
      "price_change_usd": -1.005,
      "price_change_percent": -1.11,
      "asset_details": {
        "net_asset_value_usd": 21353112007,
        "premium_discount_percent": 0.85218,
        "holding_quantity": 200088.5697,
        "change_percent_24h": 0.31,
        "change_quantity_24h": 621.2759,
        "change_percent_7d": 0.98,
        "change_quantity_7d": 1946.8863,
        "update_date": "2025-05-21"
      },
      "update_timestamp": 1762439401000
    },
    {
      "ticker": "BITB",
      "fund_name": "Bitwise Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "cik_code": "0001763415",
      "fund_type": "Spot",
      "market_cap_usd": "4188366600.00",
      "list_date": 1704931200000,
      "shares_outstanding": "75020000",
      "aum_usd": "4243051206.14",
      "management_fee_percent": "0.20",
      "last_trade_time": 1762439689775,
      "last_quote_time": 1762439689853,
      "volume_quantity": 70990,
      "volume_usd": 3959112.3,
      "price_usd": 55.83,
      "price_change_usd": -0.65,
      "price_change_percent": -1.15,
      "asset_details": {
        "net_asset_value_usd": 4243051206.14,
        "premium_discount_percent": -0.14,
        "holding_quantity": 40779.29,
        "change_percent_24h": 0,
        "change_quantity_24h": 0,
        "change_percent_7d": -1.44,
        "change_quantity_7d": -594.44,
        "update_date": "2025-11-04"
      },
      "update_timestamp": 1762439402000
    },
    {
      "ticker": "ARKB",
      "fund_name": "ARK 21Shares Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001869699",
      "fund_type": "Spot",
      "market_cap_usd": "4186619998.0",
      "list_date": 1704931200000,
      "shares_outstanding": "122774780",
      "aum_usd": "4106816399.84",
      "management_fee_percent": "0.21",
      "last_trade_time": 1762439692098,
      "last_quote_time": 1762439693958,
      "volume_quantity": 166443,
      "volume_usd": 5670713.01,
      "price_usd": 34.1,
      "price_change_usd": -0.4,
      "price_change_percent": -1.16,
      "asset_details": {
        "net_asset_value_usd": 4106816399.84,
        "premium_discount_percent": 0.3,
        "holding_quantity": 40481.107,
        "change_percent_24h": -4.17,
        "change_quantity_24h": -1761.7921,
        "change_percent_7d": -9.61,
        "change_quantity_7d": -4301.4789,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439402000
    },
    {
      "ticker": "HODL",
      "fund_name": "VanEck Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001838028",
      "fund_type": "Spot",
      "market_cap_usd": "1872897454.56",
      "list_date": 1704931200000,
      "shares_outstanding": "64493714",
      "aum_usd": "1898049976",
      "management_fee_percent": "0",
      "last_trade_time": 1762439690908,
      "last_quote_time": 1762439692989,
      "volume_quantity": 27541,
      "volume_usd": 798964.41,
      "price_usd": 29.04,
      "price_change_usd": -0.31,
      "price_change_percent": -1.06,
      "asset_details": {
        "net_asset_value_usd": 1898049976,
        "premium_discount_percent": -0.26,
        "holding_quantity": 18709.1792,
        "change_percent_24h": 7.82,
        "change_quantity_24h": 1357.6437,
        "change_percent_7d": 3.23,
        "change_quantity_7d": 585.9373,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439402000
    },
    {
      "ticker": "EZBC",
      "fund_name": "Franklin Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001992870",
      "fund_type": "Spot",
      "market_cap_usd": "594246539.4096",
      "list_date": 1704931200000,
      "shares_outstanding": "9999336",
      "aum_usd": "602060000.00",
      "management_fee_percent": "0.19",
      "last_trade_time": 1762439689105,
      "last_quote_time": 1762439693333,
      "volume_quantity": 9910,
      "volume_usd": 589238.69,
      "price_usd": 59.4286,
      "price_change_usd": -0.6614,
      "price_change_percent": -1.1,
      "asset_details": {
        "net_asset_value_usd": 602060000,
        "premium_discount_percent": -0.2,
        "holding_quantity": 5934.5373,
        "change_percent_24h": 7.21,
        "change_quantity_24h": 399.1716,
        "change_percent_7d": 2.83,
        "change_quantity_7d": 163.2903,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439403000
    },
    {
      "ticker": "BTCO",
      "fund_name": "Invesco Galaxy Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001855781",
      "fund_type": "Spot",
      "market_cap_usd": "626161680.000",
      "list_date": 1704931200000,
      "shares_outstanding": "6120000",
      "aum_usd": "749700000",
      "management_fee_percent": "0.39",
      "last_trade_time": 1762439689527,
      "last_quote_time": 1762439693941,
      "volume_quantity": 2852,
      "volume_usd": 291531.44,
      "price_usd": 102.314,
      "price_change_usd": -1.216,
      "price_change_percent": -1.17,
      "asset_details": {
        "net_asset_value_usd": 749700000,
        "premium_discount_percent": 0.11,
        "holding_quantity": 6222.5424,
        "change_percent_24h": -0.11,
        "change_quantity_24h": -6.7916,
        "change_percent_7d": 7.02,
        "change_quantity_7d": 408.0445,
        "update_date": "2025-10-03"
      },
      "update_timestamp": 1762439403000
    },
    {
      "ticker": "BTF",
      "fund_name": "Valkyrie ETF Trust II CoinShares Bitcoin and Ether ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "XNAS",
      "fund_type": "Futures",
      "market_cap_usd": "35181739.6355",
      "list_date": 1634688000000,
      "shares_outstanding": "2399273",
      "aum_usd": "35965099.6",
      "last_trade_time": 1762439647715,
      "last_quote_time": 1762439689388,
      "volume_quantity": 944,
      "volume_usd": 13895.68,
      "price_usd": 14.6635,
      "price_change_usd": -0.317,
      "price_change_percent": -2.12,
      "asset_details": {
        "net_asset_value_usd": 35965099.6,
        "premium_discount_percent": -0.03,
        "holding_quantity": 346.9131,
        "change_percent_24h": 2.7,
        "change_quantity_24h": 9.1221,
        "change_percent_7d": -2.52,
        "change_quantity_7d": -8.9794,
        "update_date": "2025-11-06"
      },
      "update_timestamp": 1762439403000
    },
    {
      "ticker": "BRRR",
      "fund_name": "Coinshares Bitcoin ETF Common Shares of Beneficial Interest",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "XNAS",
      "cik_code": "0001841175",
      "fund_type": "Spot",
      "market_cap_usd": "595494989.745",
      "list_date": 1704931200000,
      "shares_outstanding": "20537851",
      "aum_usd": "603402037.72",
      "management_fee_percent": "0.25",
      "last_trade_time": 1762439691958,
      "last_quote_time": 1762439694962,
      "volume_quantity": 2312,
      "volume_usd": 67140.48,
      "price_usd": 28.995,
      "price_change_usd": -0.335,
      "price_change_percent": -1.14,
      "asset_details": {
        "net_asset_value_usd": 603402037.72,
        "premium_discount_percent": -0.18,
        "holding_quantity": 5820.3103,
        "change_percent_24h": -0.73,
        "change_quantity_24h": -42.6259,
        "change_percent_7d": -1.47,
        "change_quantity_7d": -86.5743,
        "update_date": "2025-11-06"
      },
      "update_timestamp": 1762439404000
    },
    {
      "ticker": "BITS",
      "fund_name": "Global X Blockchain & Bitcoin Strategy ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "XNAS",
      "fund_type": "Futures",
      "market_cap_usd": "53263669",
      "list_date": 1637020800000,
      "shares_outstanding": "517123",
      "aum_usd": "55090000.00",
      "management_fee_percent": "",
      "last_trade_time": 1762439514293,
      "last_quote_time": 1762439534672,
      "volume_quantity": 2997,
      "volume_usd": 314165.3202,
      "price_usd": 103,
      "price_change_usd": -1.8266,
      "price_change_percent": -1.74,
      "asset_details": {
        "net_asset_value_usd": 55090000,
        "premium_discount_percent": 0.02,
        "holding_quantity": 509.4822,
        "update_date": "2025-10-17"
      },
      "update_timestamp": 1762439404000
    },
    {
      "ticker": "DEFI",
      "fund_name": "Hashdex Bitcoin ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "cik_code": "0001985840",
      "fund_type": "Futures",
      "market_cap_usd": "16259600.00",
      "list_date": 1711497600000,
      "shares_outstanding": "140000",
      "aum_usd": "15280000.00",
      "last_trade_time": 1762439517208,
      "last_quote_time": 1762439690677,
      "volume_quantity": 962,
      "volume_usd": 113120.0408,
      "price_usd": 116.14,
      "price_change_usd": -1.4484,
      "price_change_percent": -1.23,
      "asset_details": {
        "net_asset_value_usd": 15280000,
        "premium_discount_percent": -0.58,
        "holding_quantity": 158.2426,
        "change_percent_24h": 0,
        "change_quantity_24h": 0,
        "change_percent_7d": -0.7,
        "change_quantity_7d": -1.1109,
        "update_date": "2025-02-14"
      },
      "update_timestamp": 1762439405000
    },
    {
      "ticker": "BITC",
      "fund_name": "Bitwise Trendwise Bitcoin and Treasuries Rotation Strategy ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "fund_type": "Futures",
      "market_cap_usd": "12997574.4144",
      "list_date": 1679356800000,
      "shares_outstanding": "319357",
      "aum_usd": "22843629",
      "last_trade_time": 1762439468469,
      "last_quote_time": 1762439657853,
      "volume_quantity": 15373,
      "volume_usd": 624638.8106,
      "price_usd": 40.6992,
      "price_change_usd": 0.067,
      "price_change_percent": 0.16,
      "asset_details": {
        "net_asset_value_usd": 22843629,
        "premium_discount_percent": 0.11,
        "holding_quantity": 234.8597,
        "change_percent_24h": 4.67,
        "change_quantity_24h": 10.4739,
        "change_percent_7d": 10.5,
        "change_quantity_7d": 22.3232,
        "update_date": "2024-12-02"
      },
      "update_timestamp": 1762439405000
    },
    {
      "ticker": "BETH",
      "fund_name": "ProShares Bitcoin & Ether Market Cap Weight ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "fund_type": "Futures",
      "market_cap_usd": "13965798.0",
      "list_date": 1696204800000,
      "shares_outstanding": "210012",
      "aum_usd": "16349466.36",
      "last_trade_time": 1762439405501,
      "last_quote_time": 1762439592922,
      "volume_quantity": 2835,
      "volume_usd": 190999.9035,
      "price_usd": 66.5,
      "price_change_usd": -0.8721,
      "price_change_percent": -1.29,
      "asset_details": {
        "net_asset_value_usd": 16349466.36,
        "premium_discount_percent": 0.01,
        "holding_quantity": 156.294,
        "change_percent_24h": 15.61,
        "change_quantity_24h": 21.1067,
        "change_percent_7d": 15.27,
        "change_quantity_7d": 20.7088,
        "update_date": "2025-06-20"
      },
      "update_timestamp": 1762439405000
    },
    {
      "ticker": "BTCW",
      "fund_name": "WisdomTree Bitcoin Fund",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "BATS",
      "cik_code": "0001850391",
      "fund_type": "Spot",
      "market_cap_usd": "168346151.66",
      "list_date": 1704931200000,
      "shares_outstanding": "1550006",
      "aum_usd": "170734660.00",
      "management_fee_percent": "0.30",
      "last_trade_time": 1762439601461,
      "last_quote_time": 1762439694249,
      "volume_quantity": 809,
      "volume_usd": 88100.1,
      "price_usd": 108.61,
      "price_change_usd": -1.41,
      "price_change_percent": -1.28,
      "asset_details": {
        "net_asset_value_usd": 170734660,
        "premium_discount_percent": -0.12,
        "holding_quantity": 1682.9406,
        "change_percent_24h": 8.82,
        "change_quantity_24h": 136.4119,
        "change_percent_7d": 4.37,
        "change_quantity_7d": 70.4822,
        "update_date": "2025-11-05"
      },
      "update_timestamp": 1762439406000
    },
    {
      "ticker": "BETE",
      "fund_name": "ProShares Bitcoin & Ether Equal Weight ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "fund_type": "Futures",
      "market_cap_usd": "7380492.0",
      "list_date": 1696204800000,
      "shares_outstanding": "120008",
      "aum_usd": "7780121.63",
      "management_fee_percent": "",
      "last_trade_time": 1762439678366,
      "last_quote_time": 1762439693710,
      "volume_quantity": 3102,
      "volume_usd": 194915.1006,
      "price_usd": 61.5,
      "price_change_usd": -1.3353,
      "price_change_percent": -2.13,
      "asset_details": {
        "net_asset_value_usd": 7780121.63,
        "premium_discount_percent": 0.2,
        "holding_quantity": 74.3747,
        "change_percent_24h": -1.84,
        "change_quantity_24h": -1.3931,
        "change_percent_7d": -2.19,
        "change_quantity_7d": -1.6655,
        "update_date": "2025-06-20"
      },
      "update_timestamp": 1762439406000
    },
    {
      "ticker": "BTC",
      "fund_name": "Grayscale Bitcoin Mini Trust ETF",
      "region": "us",
      "market_status": "open",
      "primary_exchange": "ARCX",
      "cik_code": "0002015034",
      "fund_type": "Spot",
      "market_cap_usd": "4317252718.72",
      "list_date": 1722384000000,
      "shares_outstanding": "95009963",
      "aum_usd": "4053170494.33",
      "management_fee_percent": "0.15",
      "last_trade_time": 1762439683656,
      "last_quote_time": 1762439683660,
      "volume_quantity": 80184,
      "volume_usd": 3641957.28,
      "price_usd": 45.44,
      "price_change_usd": -0.54,
      "price_change_percent": -1.17,
      "asset_details": {
        "net_asset_value_usd": 4053170494.33,
        "premium_discount_percent": -0.02,
        "holding_quantity": 42100.4345,
        "change_percent_24h": 0.4,
        "change_quantity_24h": 168.2118,
        "change_percent_7d": 0.53,
        "change_quantity_7d": 220.3533,
        "update_date": "2025-02-13"
      },
      "update_timestamp": 1762439419000
    }
  ]
}

B. ETF Premium/Discount History

Endpoint: https://open-api-v4.coinglass.com/api/etf/bitcoin/premium-discount/history


This endpoint provides historical data on the premium or discount rates of Bitcoin Exchange-Traded Funds (ETFs), including Net Asset Value (NAV), market price, and premium/discount percentages for each ETF ticker.

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/etf/bitcoin/premium-discount/history?ticker=GBTC' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params
1. ticker
string
ETF ticker symbol (e.g., GBTC, IBIT).

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "nav_usd": 37.51,
      "market_price_usd": 37.51,
      "premium_discount_details": 0,
      "timestamp": 1706227200000
    },
    {
      "nav_usd": 38.57,
      "market_price_usd": 38.51,
      "premium_discount_details": -0.16,
      "timestamp": 1706486400000
    },
    {
      "nav_usd": 38.92,
      "market_price_usd": 38.87,
      "premium_discount_details": -0.13,
      "timestamp": 1706572800000
    },
    {
      "nav_usd": 37.99,
      "market_price_usd": 37.99,
      "premium_discount_details": 0,
      "timestamp": 1706659200000
    },
  ]
}


C. ETF Flows History

Endpoint: https://open-api-v4.coinglass.com/api/etf/bitcoin/flow-history

This endpoint provides historical flow data for Bitcoin Exchange-Traded Funds (ETFs), including daily net inflows and outflows in USD, closing prices, and flow breakdowns by individual ETF tickers.


cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/etf/bitcoin/flow-history \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params
-

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "timestamp": 1704931200000,
      "flow_usd": 655300000,
      "etf_flows": [
        {
          "etf_ticker": "GBTC",
          "flow_usd": -95100000
        },
        {
          "etf_ticker": "IBIT",
          "flow_usd": 111700000
        },
        {
          "etf_ticker": "FBTC",
          "flow_usd": 227000000
        },
        {
          "etf_ticker": "ARKB",
          "flow_usd": 65300000
        },
        {
          "etf_ticker": "BITB",
          "flow_usd": 237900000
        },
        {
          "etf_ticker": "BTCO",
          "flow_usd": 17400000
        },
        {
          "etf_ticker": "HODL",
          "flow_usd": 10600000
        },
    },
  ]
}

D. Aggregated Stablecoin Margin History (OHLC)

Endpoint: https://open-api-v4.coinglass.com/api/futures/open-interest/aggregated-stablecoin-history

This endpoint provides aggregated stablecoin-margined open interest data in OHLC (open, high, low, close) candlestick format.

cURL Req:
curl --request GET \
     --url 'https://open-api-v4.coinglass.com/api/futures/open-interest/aggregated-stablecoin-history?exchange_list=CME&symbol=BTC&interval=1d' \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2' \
     --header 'accept: application/json'

Query Params
2. exchange_list
string
required
Defaults to Binance
Comma-separated exchange names (e.g., "Binance,OKX,Bybit"). Retrieve supported exchanges via the 'supported-exchange-pair' API.

2. symbol
string
required
Defaults to BTC
Trading coin (e.g., BTC).Retrieve supported coins via the 'supported-coins' API.


3. interval
string
required
Defaults to 1d
Time interval for data aggregation.Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w


4. limit
int32
Number of results per request. Default: 1000, Maximum: 1000

5. start_time
int64
Start timestamp in milliseconds (e.g., 1641522717000).

6. end_time
int64
End timestamp in milliseconds (e.g., 1641522717000).


Response Json:
{
  "code": "0",
  "data": [
    {
      "time": 1676246400000,
      "open": 79394.7,
      "high": 79394.7,
      "low": 79065,
      "close": 79065
    },
    {
      "time": 1676332800000,
      "open": 79065,
      "high": 79065,
      "low": 75835.5,
      "close": 75835.5
    },
    {
      "time": 1676419200000,
      "open": 75835.5,
      "high": 76996,
      "low": 75835.5,
      "close": 76996
    },
    more..
}
