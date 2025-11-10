Exchange Rank

***Cache / Update Frequency:*** every 1 minutes for all the API plans.

***This endpoint is available on the following*** [API plans](https://www.coinglass.com/pricing)：

| Plans     | Hobbyist | Startup | Standard | Professional | Enterprise |
| :-------- | :------- | :------ | :------- | :----------- | :--------- |
| Available | ✅        | ✅       | ✅        | ✅            | ✅          |

<br />

**Response Data**

```json
{
  "code": "0",
  "data": [
    {
      "exchange": "Binance",
      "open_interest_usd": 27564494330,
      "volume_usd": 54150896832,
      "liquidation_usd_24h": 58774453.06516198
    },
    {
      "exchange": "OKX",
      "open_interest_usd": 8586569027,
      "volume_usd": 22670494849,
      "liquidation_usd_24h": 19276388.77685
    },
  ]
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
    "/api/futures/exchange-rank": {
      "get": {
        "description": "",
        "operationId": "get_new-endpoint",
        "responses": {
          "200": {
            "description": ""
          }
        },
        "parameters": []
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