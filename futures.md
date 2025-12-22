1. Price History (OHLC)

# Price History (OHLC)

This endpoint provides historical open, high, low, and close (OHLC) price data for cryptocurrencies over specified timeframes.

***Cache / Update Frequency:*** Real time for all the API plans.

***This endpoint is available on the following*** [API plans](https://www.coinglass.com/pricing)：

| Plans          | Hobbyist | Startup  | Standard | Professional | Enterprise |
| :------------- | :------- | :------- | :------- | :----------- | :--------- |
| Available      | ✅        | ✅        | ✅        | ✅            | ✅          |
| interval Limit | ​`>=4h`  | ​`>=30m` | No Limit | No Limit     | No Limit   |

# Response Data

```json
{
  "code": "0",
  "data": [
    {
      "time": 1745366400000,
      "open": "93404.9",
      "high": "93864.9",
      "low": "92730",
      "close": "92858.2",
      "volume_usd": "1166471854.3026"
    },
    {
      "time": 1745370000000,
      "open": "92858.2",
      "high": "93464.8",
      "low": "92552",
      "close": "92603.8",
      "volume_usd": "871812560.3437"
    },
    ...
 ]
}    
    
    
```

# OpenAPI definition

```json
{
  "openapi": "3.1.0",
  "info": {
    "title": "coinglass",
    "version": "3.0"
  },
  "servers": [
    {
      "url": "https://open-api-v4.coinglass.com"
    }
  ],
  "components": {
    "securitySchemes": {
      "sec0": {
        "type": "apiKey",
        "in": "header",
        "name": "CG-API-KEY"
      }
    }
  },
  "security": [
    {
      "sec0": []
    }
  ],
  "paths": {
    "/api/futures/price/history": {
      "get": {
        "summary": "Price OHLC History",
        "description": "The API retrieves historical data of the open, high, low, and close (OHLC) prices for cryptocurrencies.",
        "operationId": "price-ohlc-history",
        "parameters": [
          {
            "name": "exchange",
            "in": "query",
            "required": true,
            "description": " Futures exchange names (e.g., Binance, OKX) .Retrieve supported exchanges via the 'supported-exchange-pair' API.",
            "schema": {
              "type": "string",
              "default": "Binance"
            }
          },
          {
            "name": "symbol",
            "in": "query",
            "required": true,
            "description": "Trading pair (e.g., BTCUSDT). Check supported pairs through the 'supported-exchange-pair' API.",
            "schema": {
              "type": "string",
              "default": "BTCUSDT"
            }
          },
          {
            "name": "interval",
            "in": "query",
            "required": true,
            "description": "Data aggregation time interval. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w.",
            "schema": {
              "type": "string",
              "default": "1h"
            }
          },
          {
            "name": "limit",
            "in": "query",
            "required": true,
            "description": "Number of results per request. Default: 1000, Maximum: 1000.",
            "schema": {
              "type": "integer",
              "format": "int32",
              "default": 10
            }
          },
          {
            "name": "start_time",
            "in": "query",
            "required": false,
            "description": "Start timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "end_time",
            "in": "query",
            "required": false,
            "description": "End timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          }
        ],
        "deprecated": false,
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "application/json": {
                "examples": {
                  "New Example": {
                    "summary": "New Example",
                    "value": "{\n  \"code\": \"0\",\n  \"data\": [\n    {\n      \"time\": 1745366400000,\n      \"open\": \"93404.9\",\n      \"high\": \"93864.9\",\n      \"low\": \"92730\",\n      \"close\": \"92858.2\",\n      \"volume_usd\": \"1166471854.3026\"\n    },\n    {\n      \"time\": 1745370000000,\n      \"open\": \"92858.2\",\n      \"high\": \"93464.8\",\n      \"low\": \"92552\",\n      \"close\": \"92603.8\",\n      \"volume_usd\": \"871812560.3437\"\n    },\n    ...\n ]\n}    \n    \n    "
                  }
                }
              }
            }
          }
        }
      }
    }
  },
  "x-readme": {
    "headers": [],
    "explorer-enabled": true,
    "proxy-enabled": true
  },
  "x-readme-fauxas": true
}
```

# OpenAPI definition
```json
{
  "_id": "/branches/4.0/apis/coinglass.json",
  "openapi": "3.1.0",
  "info": {
    "title": "coinglass",
    "version": "3.0"
  },
  "servers": [
    {
      "url": "https://open-api-v4.coinglass.com"
    }
  ],
  "components": {
    "securitySchemes": {
      "sec0": {
        "type": "apiKey",
        "in": "header",
        "name": "CG-API-KEY"
      }
    }
  },
  "security": [
    {
      "sec0": []
    }
  ],
  "paths": {
    "/api/futures/price/history": {
      "get": {
        "summary": "Price OHLC History",
        "description": "The API retrieves historical data of the open, high, low, and close (OHLC) prices for cryptocurrencies.",
        "operationId": "price-ohlc-history",
        "parameters": [
          {
            "name": "exchange",
            "in": "query",
            "required": true,
            "description": " Futures exchange names (e.g., Binance, OKX) .Retrieve supported exchanges via the 'supported-exchange-pair' API.",
            "schema": {
              "type": "string",
              "default": "Binance"
            }
          },
          {
            "name": "symbol",
            "in": "query",
            "required": true,
            "description": "Trading pair (e.g., BTCUSDT). Check supported pairs through the 'supported-exchange-pair' API.",
            "schema": {
              "type": "string",
              "default": "BTCUSDT"
            }
          },
          {
            "name": "interval",
            "in": "query",
            "required": true,
            "description": "Data aggregation time interval. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w.",
            "schema": {
              "type": "string",
              "default": "1h"
            }
          },
          {
            "name": "limit",
            "in": "query",
            "required": true,
            "description": "Number of results per request. Default: 1000, Maximum: 1000.",
            "schema": {
              "type": "integer",
              "format": "int32",
              "default": 10
            }
          },
          {
            "name": "start_time",
            "in": "query",
            "required": false,
            "description": "Start timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "end_time",
            "in": "query",
            "required": false,
            "description": "End timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          }
        ],
        "deprecated": false,
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "application/json": {
                "examples": {
                  "New Example": {
                    "summary": "New Example",
                    "value": "{\n  \"code\": \"0\",\n  \"data\": [\n    {\n      \"time\": 1745366400000,\n      \"open\": \"93404.9\",\n      \"high\": \"93864.9\",\n      \"low\": \"92730\",\n      \"close\": \"92858.2\",\n      \"volume_usd\": \"1166471854.3026\"\n    },\n    {\n      \"time\": 1745370000000,\n      \"open\": \"92858.2\",\n      \"high\": \"93464.8\",\n      \"low\": \"92552\",\n      \"close\": \"92603.8\",\n      \"volume_usd\": \"871812560.3437\"\n    },\n    ...\n ]\n}    \n    \n    "
                  }
                }
              }
            }
          }
        }
      }
    }
  },
  "x-readme": {
    "headers": [],
    "explorer-enabled": true,
    "proxy-enabled": true
  },
  "x-readme-fauxas": true
}
```


2. Coin Taker Buy/Sell History

# Coin Taker Buy/Sell History

This endpoint provides historical data for the long/short ratio of aggregated taker buy and sell volumes for futures cryptocurrencies.

***This endpoint is available on the following*** [API plans](https://www.coinglass.com/pricing)：

| Plans          | Hobbyist | Startup  | Standard | Professional | Enterprise |
| :------------- | :------- | :------- | :------- | :----------- | :--------- |
| Available      | ✅        | ✅        | ✅        | ✅            | ✅          |
| interval Limit | ​`>=4h`  | ​`>=30m` | No Limit | No Limit     | No Limit   |

&#x20;

# Response Data

```json
{
  "code": "0",
  "msg": "success",
  "data": [
    {
      "time": 1741622400000, // Timestamp in milliseconds
      "aggregated_buy_volume_usd": 968834542.3787, // Aggregated buy volume (USD)
      "aggregated_sell_volume_usd": 1054582654.8138 // Aggregated sell volume (USD)
    },
    {
      "time": 1741626000000,
      "aggregated_buy_volume_usd": 1430620763.2041,
      "aggregated_sell_volume_usd": 1559166911.2821
    },
    {
      "time": 1741629600000,
      "aggregated_buy_volume_usd": 1897261721.0129,
      "aggregated_sell_volume_usd": 2003812276.7812
    }
  ]
}

```

# OpenAPI definition

```json
{
  "openapi": "3.1.0",
  "info": {
    "title": "coinglass",
    "version": "3.0"
  },
  "servers": [
    {
      "url": "https://open-api-v4.coinglass.com"
    }
  ],
  "components": {
    "securitySchemes": {
      "sec0": {
        "type": "apiKey",
        "in": "header",
        "name": "CG-API-KEY"
      }
    }
  },
  "security": [
    {
      "sec0": []
    }
  ],
  "paths": {
    "/api/futures/aggregated-taker-buy-sell-volume/history": {
      "get": {
        "summary": "Aggregated Taker Buy/Sell Volume History",
        "description": "This API retrieves historical data for the long/short ratio of aggregated taker buy/sell volumes.",
        "operationId": "aggregated-taker-buysell-volume-history",
        "parameters": [
          {
            "name": "exchange_list",
            "in": "query",
            "required": true,
            "description": "exchange_list: List of exchange names to retrieve data from (e.g., 'Binance,OKX,Bybit')",
            "schema": {
              "type": "string",
              "default": "Binance"
            }
          },
          {
            "name": "symbol",
            "in": "query",
            "required": true,
            "description": "Trading pair (e.g., BTC). Retrieve supported coins via the 'supported-coins' API.",
            "schema": {
              "type": "string",
              "default": "BTC"
            }
          },
          {
            "name": "interval",
            "in": "query",
            "required": true,
            "description": "Time interval for data aggregation.  Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w",
            "schema": {
              "type": "string",
              "default": "h1"
            }
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Number of results per request.  Default: 1000, Maximum: 1000",
            "schema": {
              "type": "integer",
              "format": "int32",
              "default": "10"
            }
          },
          {
            "name": "start_time",
            "in": "query",
            "required": false,
            "description": "Start timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "end_time",
            "in": "query",
            "required": false,
            "description": "End timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "unit",
            "in": "query",
            "required": false,
            "description": "Unit for the returned data, choose between 'usd' or 'coin'.",
            "schema": {
              "type": "string",
              "default": "usd"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "200",
            "content": {
              "application/json": {
                "examples": {
                  "Result": {
                    "value": "{\n  \"code\": \"0\",\n  \"msg\": \"success\",\n  \"data\": [\n    {\n      \"time\": 1741622400000, // Timestamp in milliseconds\n      \"aggregated_buy_volume_usd\": 968834542.3787, // Aggregated buy volume (USD)\n      \"aggregated_sell_volume_usd\": 1054582654.8138 // Aggregated sell volume (USD)\n    },\n    {\n      \"time\": 1741626000000,\n      \"aggregated_buy_volume_usd\": 1430620763.2041,\n      \"aggregated_sell_volume_usd\": 1559166911.2821\n    },\n    {\n      \"time\": 1741629600000,\n      \"aggregated_buy_volume_usd\": 1897261721.0129,\n      \"aggregated_sell_volume_usd\": 2003812276.7812\n    }\n  ]\n}\n"
                  }
                },
                "schema": {
                  "type": "object",
                  "properties": {}
                }
              }
            }
          },
          "400": {
            "description": "400",
            "content": {
              "application/json": {
                "examples": {
                  "Result": {
                    "value": "{}"
                  }
                },
                "schema": {
                  "type": "object",
                  "properties": {}
                }
              }
            }
          }
        },
        "deprecated": false
      }
    }
  },
  "x-readme": {
    "headers": [],
    "explorer-enabled": true,
    "proxy-enabled": true
  },
  "x-readme-fauxas": true
}
```

# OpenAPI definition
```json
{
  "_id": "/branches/4.0/apis/coinglass.json",
  "openapi": "3.1.0",
  "info": {
    "title": "coinglass",
    "version": "3.0"
  },
  "servers": [
    {
      "url": "https://open-api-v4.coinglass.com"
    }
  ],
  "components": {
    "securitySchemes": {
      "sec0": {
        "type": "apiKey",
        "in": "header",
        "name": "CG-API-KEY"
      }
    }
  },
  "security": [
    {
      "sec0": []
    }
  ],
  "paths": {
    "/api/futures/aggregated-taker-buy-sell-volume/history": {
      "get": {
        "summary": "Aggregated Taker Buy/Sell Volume History",
        "description": "This API retrieves historical data for the long/short ratio of aggregated taker buy/sell volumes.",
        "operationId": "aggregated-taker-buysell-volume-history",
        "parameters": [
          {
            "name": "exchange_list",
            "in": "query",
            "required": true,
            "description": "exchange_list: List of exchange names to retrieve data from (e.g., 'Binance,OKX,Bybit')",
            "schema": {
              "type": "string",
              "default": "Binance"
            }
          },
          {
            "name": "symbol",
            "in": "query",
            "required": true,
            "description": "Trading pair (e.g., BTC). Retrieve supported coins via the 'supported-coins' API.",
            "schema": {
              "type": "string",
              "default": "BTC"
            }
          },
          {
            "name": "interval",
            "in": "query",
            "required": true,
            "description": "Time interval for data aggregation.  Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w",
            "schema": {
              "type": "string",
              "default": "h1"
            }
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "description": "Number of results per request.  Default: 1000, Maximum: 1000",
            "schema": {
              "type": "integer",
              "format": "int32",
              "default": "10"
            }
          },
          {
            "name": "start_time",
            "in": "query",
            "required": false,
            "description": "Start timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "end_time",
            "in": "query",
            "required": false,
            "description": "End timestamp in milliseconds (e.g., 1641522717000).",
            "schema": {
              "type": "integer",
              "format": "int64",
              "default": ""
            }
          },
          {
            "name": "unit",
            "in": "query",
            "required": false,
            "description": "Unit for the returned data, choose between 'usd' or 'coin'.",
            "schema": {
              "type": "string",
              "default": "usd"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "200",
            "content": {
              "application/json": {
                "examples": {
                  "Result": {
                    "value": "{\n  \"code\": \"0\",\n  \"msg\": \"success\",\n  \"data\": [\n    {\n      \"time\": 1741622400000, // Timestamp in milliseconds\n      \"aggregated_buy_volume_usd\": 968834542.3787, // Aggregated buy volume (USD)\n      \"aggregated_sell_volume_usd\": 1054582654.8138 // Aggregated sell volume (USD)\n    },\n    {\n      \"time\": 1741626000000,\n      \"aggregated_buy_volume_usd\": 1430620763.2041,\n      \"aggregated_sell_volume_usd\": 1559166911.2821\n    },\n    {\n      \"time\": 1741629600000,\n      \"aggregated_buy_volume_usd\": 1897261721.0129,\n      \"aggregated_sell_volume_usd\": 2003812276.7812\n    }\n  ]\n}\n"
                  }
                },
                "schema": {
                  "type": "object",
                  "properties": {}
                }
              }
            }
          },
          "400": {
            "description": "400",
            "content": {
              "application/json": {
                "examples": {
                  "Result": {
                    "value": "{}"
                  }
                },
                "schema": {
                  "type": "object",
                  "properties": {}
                }
              }
            }
          }
        },
        "deprecated": false
      }
    }
  },
  "x-readme": {
    "headers": [],
    "explorer-enabled": true,
    "proxy-enabled": true
  },
  "x-readme-fauxas": true
}
```