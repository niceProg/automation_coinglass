| Interval | 1 record = | Total Records (±365 hari) | API Calls (2000 limit) |
| -------- | ---------- | ------------------------- | ---------------------- |
| **1m**   | 1 menit    | 525 600                   | **263 calls**          |
| **3m**   | 3 menit    | 175 200                   | **88 calls**           |
| **5m**   | 5 menit    | 105 120                   | **53 calls**           |
| **15m**  | 15 menit   | 35 040                    | **18 calls**           |
| **30m**  | 30 menit   | 17 520                    | **9 calls**            |
| **1h**   | 60 menit   | 8 760                     | **5 calls**            |
| **4h**   | 240 menit  | 2 190                     | **2 calls**            |
| **6h**   | 360 menit  | 1 460                     | **1 call** ✅           |
| **8h**   | 480 menit  | 1 095                     | **1 call** ✅           |
| **12h**  | 720 menit  | 730                       | **1 call** ✅           |
| **1d**   | 1 hari     | 365                       | **1 call** ✅           |
| **1w**   | 7 hari     | 52                        | **1 call** ✅           |

example:
import time, requests

API = "https://open-api-v4.coinglass.com/api/futures/open-interest/aggregated-history"
HEAD = {"coinglassSecret": "YOUR_API_KEY"}
symbol = "BTC"
interval = "1h"

# 1 tahun (ms)
year_ms = 365 * 24 * 60 * 60 * 1000
now = int(time.time() * 1000)
start_limit = now - year_ms

batch_span = (2000 * 60 * 60 * 1000)  # 2000 data × 1h
end_time = now
all_data = []

while end_time > start_limit:
    start_time = max(end_time - batch_span, start_limit)
    params = {
        "symbol": symbol,
        "interval": interval,
        "start_time": start_time,
        "end_time": end_time,
        "limit": 2000
    }
    r = requests.get(API, params=params, headers=HEAD)
    data = r.json().get("data", [])
    if not data:
        break
    all_data = data + all_data  # prepend (karena mundur)
    print(f"{interval}: got {len(data)} rows, from {data[0]['time']} to {data[-1]['time']}")
    end_time = start_time - 1
