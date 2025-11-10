# Coinglass API Endpoints Documentation

This document provides detailed information about the funding rate history and open interest history endpoints from the Coinglass API.

## Table of Contents
- [Authentication](#authentication)
- [Funding Rate History Endpoints](#funding-rate-history-endpoints)
- [Open Interest History Endpoints](#open-interest-history-endpoints)
- [Database Schema](#database-schema)
- [Pipeline Configuration](#pipeline-configuration)
- [Error Handling](#error-handling)

---

## Authentication

All API calls require authentication via an API key:

**Header:** `CG-API-KEY: your_api_key_here`

**Base URL:** `https://open-api-v4.coinglass.com/api`

---

## Funding Rate History Endpoints

### 1. Get Funding Rate History

**Endpoint:** `GET /futures/funding-rate/history`

**Description:** Retrieves historical funding rate data for a specific exchange and trading pair.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `exchange` | string | ✅ | - | Exchange name (e.g., "Binance", "OKX") |
| `symbol` | string | ✅ | - | Trading pair (e.g., "BTCUSDT") |
| `interval` | string | ❌ | "1h" | Time interval (e.g., "1h", "4h", "8h", "1d") |
| `limit` | integer | ❌ | 1000 | Number of results (max: 1000) |
| `start_time` | integer | ❌ | null | Start timestamp in milliseconds |
| `end_time` | integer | ❌ | null | End timestamp in milliseconds |

**Example Request:**
```python
params = {
    "exchange": "Binance",
    "symbol": "BTCUSDT",
    "interval": "1h",
    "limit": 1000,
    "start_time": 1641522717000,
    "end_time": 1641609117000
}
```

**Response Structure:**
```json
[
    {
        "time": 1641522717000,
        "open": 0.0001,
        "high": 0.0002,
        "low": 0.00005,
        "close": 0.00015
    }
]
```

### 2. Get Funding Rate Exchange List

**Endpoint:** `GET /futures/funding-rate/exchange-list`

**Description:** Retrieves current funding rates across multiple exchanges for a specific symbol.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | ✅ | Trading symbol (e.g., "BTC") |

**Response Structure:**
```json
[
    {
        "symbol": "BTC",
        "stablecoin_margin_list": [
            {
                "exchange": "Binance",
                "funding_rate": 0.0001,
                "funding_rate_interval": 8,
                "next_funding_time": 1641522717000
            }
        ],
        "token_margin_list": [
            {
                "exchange": "Binance",
                "funding_rate": 0.0001,
                "funding_rate_interval": 8,
                "next_funding_time": 1641522717000
            }
        ]
    }
]
```

---

## Open Interest History Endpoints

### 1. Get Open Interest History

**Endpoint:** `GET /futures/open-interest/history`

**Description:** Retrieves historical open interest data for a specific exchange and trading pair.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `exchange` | string | ✅ | - | Exchange name (e.g., "Binance", "OKX") |
| `symbol` | string | ✅ | - | Trading pair (e.g., "BTCUSDT") |
| `interval` | string | ❌ | "1h" | Time interval (e.g., "1m", "5m", "15m", "1h", "4h", "1d") |
| `limit` | integer | ❌ | 1000 | Number of results (max: 1000) |
| `start_time` | integer | ❌ | null | Start timestamp in milliseconds |
| `end_time` | integer | ❌ | null | End timestamp in milliseconds |
| `unit` | string | ❌ | null | Unit type ("usd" or "coin") |

**Example Request:**
```python
params = {
    "exchange": "Binance",
    "symbol": "BTCUSDT",
    "interval": "1h",
    "limit": 1000,
    "unit": "usd",
    "start_time": 1641522717000,
    "end_time": 1641609117000
}
```

**Response Structure:**
```json
[
    {
        "time": 1641522717000,
        "open": 1000000.0,
        "high": 1200000.0,
        "low": 950000.0,
        "close": 1100000.0
    }
]
```

### 2. Get Open Interest Exchange List

**Endpoint:** `GET /futures/open-interest/exchange-list`

**Description:** Retrieves current open interest data across multiple exchanges for a specific symbol.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | ✅ | Trading symbol (e.g., "BTC") |

**Response Structure:**
```json
[
    {
        "exchange": "Binance",
        "open_interest_usd": 1000000.0,
        "open_interest_quantity": 16.5,
        "open_interest_change_percent_5m": 0.5,
        "open_interest_change_percent_15m": 1.2,
        "open_interest_change_percent_30m": 2.1,
        "open_interest_change_percent_1h": 3.5,
        "open_interest_change_percent_4h": 8.7,
        "open_interest_change_percent_24h": 15.2
    }
]
```

### 3. Get Open Interest Exchange History Chart

**Endpoint:** `GET /futures/open-interest/exchange-history-chart`

**Description:** Retrieves historical open interest chart data for a specific symbol across exchanges.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `symbol` | string | ✅ | - | Trading symbol (e.g., "BTC") |
| `range` | string | ❌ | "12h" | Time range (e.g., "12h", "1d", "1w") |
| `unit` | string | ❌ | null | Unit type ("usd" or "coin") |

---

## Database Schema

### Funding Rate History Table

**Table Name:** `cg_funding_rate_history`

```sql
CREATE TABLE cg_funding_rate_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    exchange VARCHAR(50) NOT NULL,
    pair VARCHAR(50) NOT NULL,
    interval VARCHAR(10) NOT NULL,
    time BIGINT NOT NULL,
    open DECIMAL(18,8),
    high DECIMAL(18,8),
    low DECIMAL(18,8),
    close DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, interval, time)
);
```

### Open Interest History Table

**Table Name:** `cg_open_interest_history`

```sql
CREATE TABLE cg_open_interest_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    exchange VARCHAR(50) NOT NULL,
    pair VARCHAR(50) NOT NULL,
    interval VARCHAR(10) NOT NULL,
    time BIGINT NOT NULL,
    open DECIMAL(38,8),
    high DECIMAL(38,8),
    low DECIMAL(38,8),
    close DECIMAL(38,8),
    unit VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, interval, time)
);
```

---

## Pipeline Configuration

### Funding Rate Pipeline

**File:** `app/providers/coinglass/pipelines/funding_rate.py`

**Default Configuration:**
- **Timeframes:** `["1h", "8h"]`
- **Symbols:** `["BTC", "ETH", "BNB", "SOL", "XRP"]`
- **Exchanges:** `["Binance", "OKX"]`
- **Limit:** `1000`

### Open Interest Pipeline

**File:** `app/providers/coinglass/pipelines/open_interest.py`

**Default Configuration:**
- **Timeframes:** `["1m", "5m", "15m"]`
- **Symbols:** `["BTC", "ETH", "BNB", "SOL", "XRP"]`
- **Exchanges:** `["Binance", "OKX"]`
- **Limit:** `1000`
- **Unit:** `"usd"`

---

## Error Handling

The system implements robust error handling with:

- **Timeout Configuration:** 30 seconds for API calls
- **Retry Logic:** Automatic retries for network failures
- **Logging:** Comprehensive error logging for debugging
- **Validation:** Parameter validation before API calls

**Common Error Responses:**
- `401 Unauthorized`: Invalid API key
- `400 Bad Request`: Invalid parameters
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: API server issues

---

## Usage Examples

### Python Client Usage

```python
from app.providers.coinglass.client import CoinglassClient

# Initialize client
client = CoinglassClient(api_key="your_api_key")

# Get funding rate history
funding_data = client.get_fr_history(
    exchange="Binance",
    symbol="BTCUSDT",
    interval="1h",
    limit=500
)

# Get open interest history
oi_data = client.get_oi_history(
    exchange="Binance",
    symbol="BTCUSDT",
    interval="1h",
    limit=500,
    unit="usd"
)
```

---

## Notes

- All timestamps are in milliseconds (Unix timestamp × 1000)
- Rate limits may apply - check the Coinglass API documentation for current limits
- The system automatically handles data deduplication using unique constraints
- Historical data availability depends on the exchange and time period requested