A. FRED Macro Overlay API Cheat Sheet
=================================

Endpoint
--------
Base URL: https://api.stlouisfed.org/fred/series/observations
Authentication: Append `api_key=6430aed90a9710e51f71603bc00d00d5` as a query parameter.

Primary Query Parameters
------------------------
- `series_id` (required): One of DTWEXBGS (DXY), DGS10 (10Y yield), DGS2 (2Y yield), DFF (Fed Funds), CPIAUCSL (CPI), PAYEMS (NFP), M2SL (M2), RRPONTSYD (ON RRP), WTREGEN (TGA).
- `observation_start` / `observation_end`: ISO dates to clip range (e.g., `2024-01-01`).
- `limit`: Max rows to return (default 1,000; set to 1 for "latest" pull).
- `sort_order`: `asc` or `desc` (use `desc` for latest-first).
- `frequency`: Force aggregation cadence (e.g., `d`, `w`, `m`).
- `file_type`: `json`, `xml`, or `txt` (JSON recommended).

cURL Example (latest DXY)
------------------------
```
curl "https://api.stlouisfed.org/fred/series/observations?series_id=DTWEXBGS&api_key=6430aed90a9710e51f71603bc00d00d5&file_type=json&limit=1&sort_order=desc"
```

Sample JSON Response (truncated)
--------------------------------
```
{
  "realtime_start": "2025-11-07",
  "realtime_end": "2025-11-07",
  "observation_start": "1776-01-01",
  "observation_end": "9999-12-31",
  "observations": [
    {
      "date": "2025-10-31",
      "value": "121.7715"
    }
  ]
}
```

FRED’s series/observations endpoint returns JSON shaped like this (shown with
  the DXY request):

  {
    "realtime_start": "2025-11-07",
    "realtime_end": "2025-11-07",
    "observation_start": "1776-01-01",
    "observation_end": "9999-12-31",
    "units": "lin",
    "output_type": 1,
    "file_type": "json",
    "order_by": "observation_date",
    "sort_order": "asc",
    "count": 4862,
    "offset": 0,
    "limit": 1,
    "observations": [
      {
        "realtime_start": "2025-11-07",
        "realtime_end": "2025-11-07",
        "date": "2025-10-31",
        "value": "121.7715"
      }
    ]
  }

  Key pieces:

  - Top-level metadata tells you the real-time revision window, units, count,
    pagination, etc.
  - The actual data lives in the observations array—each item has the date and
    the string value (convert to float yourself). Some series include extra
    fields (e.g., footnotes or cpi_percent_change) but the core always includes
    date/value.

Operational Goals & Uses
------------------------
1. Liquidity & Dollar Strength: Track DXY (USD strength) against BTC correlation targets.
2. Rate Regime Monitoring: Use DGS10, DGS2, and DFF to flag steepening/flattening and funding cost inflection points.
3. Inflation & Growth Pulse: CPIAUCSL and PAYEMS feed into macro-risk toggles (risk-on/off) and event dashboards.
4. Liquidity Buckets: M2SL, RRPONTSYD, WTREGEN quantify liquidity sources/sinks (bank reserves, money market uptake, Treasury cash).
5. Cadence Alignment: Combine daily series (DXY, yields, RRP) with monthly (CPI, M2, TGA) and event-driven (NFP) charts for the Macro Overlay module. Use `observation_start`, `frequency`, and `limit` to normalize windows before merging.

Tips
----
- All values are strings; cast to float and treat "." or "" as missing.
- Rate series are in percent; CPI is index level (1982-84=100); PAYEMS in thousands; M2, TGA, RRP in billions USD (check `units` field from `fred/series` if needed).
- For bulk pulls, iterate over `series_id` list and store `{date, metric, value}` to drive the Macro Overlay raw dashboard.


B. Bitcoin vs Global M2 Supply & Growth

Endpoint: https://open-api-v4.coinglass.com/api/index/bitcoin-vs-global-m2-growth

cURL Req:
curl --request GET \
     --url https://open-api-v4.coinglass.com/api/index/bitcoin-vs-global-m2-growth \
     --header 'CG-API-KEY: f78a531eb0ef4d06ba9559ec16a6b0c2'

Query Params:
-

Response JSON Example:
{
  "code": "0",
  "data": [
    {
      "timestamp": 1369008000000,
      "price": 122.5,
      "global_m2_yoy_growth": 5.5654014717,
      "global_m2_supply": 59166686473105
    },
    {
      "timestamp": 1369612800000,
      "price": 133.5,
      "global_m2_yoy_growth": 5.8571414345,
      "global_m2_supply": 59490171457382
    },
    {
      "timestamp": 1370217600000,
      "price": 122.5,
      "global_m2_yoy_growth": 6.7670545757,
      "global_m2_supply": 60072386054198
    },
    {
      "timestamp": 1370822400000,
      "price": 100.43699646,
      "global_m2_yoy_growth": 7.2970858354,
      "global_m2_supply": 60559839608422
    },
  ]
}