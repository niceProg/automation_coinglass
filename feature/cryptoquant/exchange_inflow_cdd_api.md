# CryptoQuant Exchange Inflow CDD API Documentation

## Overview

**Exchange Inflow CDD (Coin Days Destroyed)** mengukur "umur" dari koin Bitcoin yang masuk ke exchange. CDD tinggi menunjukkan koin lama (long-term holder) bergerak ke exchange, yang dapat mengindikasikan potensi selling pressure dan distribusi.

**Trading Interpretation:**
- **CDD Tinggi** → Old coins masuk exchange → Potensi selling pressure → Bearish signal
- **CDD Rendah** → Mostly young coins moving → Normal trading activity → Healthy market
- **CDD Spike** → Long-term holders mulai distribute → Volatilitas tinggi diperkirakan

---

## API Endpoints

### 1. Exchange Inflow CDD

**Laravel Backend Endpoint:**
```
GET /api/cryptoquant/exchange-inflow-cdd
```

**CryptoQuant Direct API:**
```
GET https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-inflow-cdd
```

#### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string | No | 7 days ago | Start date in `YYYY-MM-DD` format |
| `end_date` | string | No | Today | End date in `YYYY-MM-DD` format |
| `exchange` | string | No | `binance` | Exchange name or aggregation type |
| `interval` | string | No | `1d` | Data interval: `1h`, `4h`, `1d`, `1w` |

#### Exchange Options

**Aggregated Exchanges:**
- `all_exchange` - All supported exchanges (complete aggregation)
- `spot_exchange` - Spot exchanges only
- `derivative_exchange` - Derivative exchanges only

**Individual Exchanges:**
- `binance` - Binance
- `kraken` - Kraken
- `bybit` - Bybit
- `gemini` - Gemini
- `bitfinex` - Bitfinex
- `kucoin` - KuCoin
- `bitstamp` - Bitstamp
- `mexc` - MEXC

#### Request Example

```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2024-11-01&end_date=2024-11-07&exchange=binance&interval=1d"
```

#### Response Format

**Success Response:**
```json
{
  "success": true,
  "data": [
    {
      "date": "2024-11-01",
      "value": 1234567.89
    },
    {
      "date": "2024-11-02",
      "value": 2345678.90
    }
  ],
  "meta": {
    "start_date": "2024-11-01",
    "end_date": "2024-11-07",
    "exchange": "binance",
    "requested_exchange": "binance",
    "count": 7,
    "source": "CryptoQuant Exchange Inflow CDD - Real Data Only",
    "latest_data_date": "2024-11-07",
    "is_native_all_exchange": false,
    "note": "Single exchange data"
  }
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Internal server error",
  "message": "Error details here"
}
```

#### Response Fields

**Data Array:**
- `date` (string): Date in `YYYY-MM-DD` format
- `value` (float): Exchange Inflow CDD value (Coin Days Destroyed)

**Meta Object:**
- `start_date` (string): Start date used
- `end_date` (string): End date used
- `exchange` (string): Exchange identifier used
- `requested_exchange` (string): Original exchange parameter
- `count` (integer): Number of data points returned
- `source` (string): Data source identifier
- `latest_data_date` (string): Date of most recent data point
- `is_native_all_exchange` (boolean): Whether using native aggregation
- `note` (string): Additional information about data source

---

### 2. Bitcoin Market Price (Price Overlay)

**Laravel Backend Endpoint:**
```
GET /api/cryptoquant/btc-market-price
```

#### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string | No | 30 days ago | Start date in `YYYY-MM-DD` format |
| `end_date` | string | No | Today | End date in `YYYY-MM-DD` format |

#### Request Example

```bash
curl "http://127.0.0.1:8001/api/cryptoquant/btc-market-price?start_date=2024-11-01&end_date=2024-11-07"
```

#### Response Format

```json
{
  "success": true,
  "data": [
    {
      "date": "2024-11-01",
      "close": 45000.50,
      "open": 44500.00,
      "high": 45500.00,
      "low": 44000.00
    }
  ],
  "meta": {
    "start_date": "2024-11-01",
    "end_date": "2024-11-07",
    "count": 7,
    "source": "CryptoQuant Bitcoin Market Price"
  }
}
```

**Note:** Endpoint ini digunakan untuk price overlay pada CDD chart untuk memberikan konteks pergerakan CDD relatif terhadap harga Bitcoin.

---

## Implementation Details

### Backend Controller

**File:** `app/Http/Controllers/CryptoQuantController.php`

**Method:** `getExchangeInflowCDD(Request $request)`

**Functionality:**
1. Accepts query parameters: `start_date`, `end_date`, `exchange`, `interval`
2. Converts date format from `YYYY-MM-DD` to `YYYYMMDD` for CryptoQuant API
3. Calculates `limit` based on date range
4. Fetches data from CryptoQuant API
5. Transforms response to standardized format
6. Returns JSON response with data and meta information

**Key Methods:**
- `fetchSingleExchangeCDD()` - Fetches CDD data for single exchange
- `fetchAllExchangesCDD()` - Aggregates CDD data from multiple exchanges
- `aggregateCDDByDate()` - Aggregates CDD values by date across exchanges

### Frontend Controller

**File:** `public/js/exchange-inflow-cdd-controller.js`

**Functionality:**
1. Fetches CDD data and price data in parallel
2. Calculates statistical metrics
3. Renders charts with Chart.js
4. Generates market signals based on z-score analysis

**Key Methods:**
- `loadData()` - Main data loading function
- `fetchCDDData()` - Fetches CDD data from API
- `loadPriceData()` - Fetches Bitcoin price data
- `calculateMetrics()` - Calculates all statistical metrics
- `calculateMarketSignal()` - Generates trading signal
- `renderChart()` - Renders main CDD chart
- `renderDistributionChart()` - Renders histogram
- `renderMAChart()` - Renders moving averages chart

### Calculated Metrics

**Basic Metrics:**
- `currentCDD` - Latest CDD value
- `cddChange` - 24h percentage change
- `avgCDD` - Average CDD for the period
- `medianCDD` - Median CDD value
- `maxCDD` - Peak CDD value
- `peakDate` - Date when max CDD occurred

**Moving Averages:**
- `ma7` - 7-day moving average
- `ma30` - 30-day moving average

**Outlier Detection:**
- `highCDDEvents` - Count of values > 2σ (standard deviations above mean)
- `extremeCDDEvents` - Count of values > 3σ

**Market Signal:**
- Calculated using z-score: `zScore = (currentCDD - avgCDD) / stdDev`
- Signal levels:
  - `zScore > 2` → "Distribution" (Strong) - Old coins moving to exchanges
  - `zScore > 1` → "Caution" (Moderate) - Elevated CDD levels
  - `zScore < -1` → "Accumulation" (Weak) - Low distribution activity
  - `-1 ≤ zScore ≤ 1` → "Neutral" (Normal) - Normal market conditions

---

## Usage Examples

### Example 1: Get Last 7 Days CDD for Binance
```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2024-11-01&end_date=2024-11-07&exchange=binance&interval=1d"
```

### Example 2: Get All Exchanges Aggregated CDD
```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2024-11-01&end_date=2024-11-07&exchange=all_exchange&interval=1d"
```

### Example 3: Get Spot Exchanges Only
```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2024-11-01&end_date=2024-11-07&exchange=spot_exchange&interval=1d"
```

### Example 4: Get Hourly Data for Last 24 Hours
```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2024-11-06&end_date=2024-11-07&exchange=binance&interval=1h"
```

### Example 5: Get Historical Data (1 Year)
```bash
curl "http://127.0.0.1:8001/api/cryptoquant/exchange-inflow-cdd?start_date=2023-11-07&end_date=2024-11-07&exchange=binance&interval=1d"
```

---

## Dashboard Features

### Summary Cards
1. **BTC/USD** - Current Bitcoin price and 24h change
2. **CDD Saat Ini** - Current CDD value and 24h change
3. **Rata-rata Periode** - Average and median CDD
4. **CDD Tertinggi** - Peak CDD value and date
5. **Sinyal Market** - Automated trading signal based on z-score

### Charts
1. **Main CDD Chart** - Line/Bar chart with dual Y-axis (CDD + Price overlay)
2. **Distribution Chart** - Histogram showing CDD value distribution
3. **Moving Averages Chart** - MA7 and MA30 visualization

### Filters
- **Exchange Selector:** Choose exchange or aggregation type (all_exchange, spot_exchange, derivative_exchange, binance, kraken, bybit, gemini, bitfinex, kucoin, bitstamp, mexc)
- **Time Range:** 1D, 7D, 1M, YTD, 1Y, ALL
- **Chart Interval:** 1H, 4H, 1D, 1W
- **Scale Type:** Linear or Logarithmic
- **Chart Type:** Line or Bar

---

## Technical Notes

1. **API Key:** Stored in `CryptoQuantController::$apiKey`
2. **Date Format:** Backend accepts `YYYY-MM-DD`, converts to `YYYYMMDD` for CryptoQuant API
3. **Data Transformation:** CryptoQuant response is transformed to standardized format with `date` and `value` fields
4. **Error Handling:** All errors are logged and returned with descriptive messages
5. **Caching:** No caching implemented - real-time data fetching

---

## Related Files

- **Backend Controller:** `app/Http/Controllers/CryptoQuantController.php`
- **Frontend View:** `resources/views/derivatives/exchange-inflow-cdd.blade.php`
- **Frontend Controller:** `public/js/exchange-inflow-cdd-controller.js`
- **Chart Renderer:** `public/js/cdd-chart-safe.js`
- **API Route:** `routes/web.php` (route: `/api/cryptoquant/exchange-inflow-cdd`)

---

## References

- **CryptoQuant API Docs:** https://docs.cryptoquant.com/
- **Coin Days Destroyed Concept:** https://academy.binance.com/en/articles/coin-days-destroyed-cdd
- **Exchange Inflow CDD:** Measures the age of coins flowing into exchanges

API_KEY_CRYPTOQUANT=jED5yIBUPyzpeRTodjcSPGiltvvdAaJQmV1op1ED3v4UkDorgm6O20rRTq3yKWloyebmxw