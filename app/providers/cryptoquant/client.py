import requests
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from datetime import datetime, timedelta
from app.core.logging import setup_logger
from app.core.config import Settings


class CryptoQuantClient:
    """CryptoQuant API v1 Client"""

    BASE_URL = "https://api.cryptoquant.com/v1"

    def __init__(self):
        self.logger = setup_logger(__name__)
        cfg = Settings()
        self.api_key = cfg.CRYPTOQUANT_API_KEY
        if not self.api_key:
            raise ValueError("CRYPTOQUANT_API_KEY is required")
        self.headers = {"accept": "application/json", "Authorization": f"Bearer {self.api_key}"}

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.BASE_URL}/{endpoint}"
        if params:
            params = {k: v for k, v in params.items() if v is not None and v != ""}
            if params:
                url = f"{url}?{urlencode(params)}"

        try:
            self.logger.debug(f"[CryptoQuant] GET {url}")

            # Try with different timeout strategies
            for attempt in range(3):
                try:
                    # Progressive timeout: 15s -> 20s -> 25s
                    timeout = 15 + (attempt * 5)
                    self.logger.debug(f"[CryptoQuant] Attempt {attempt + 1}/3 with timeout {timeout}s")

                    resp = requests.get(
                        url,
                        headers=self.headers,
                        timeout=timeout,
                        verify=True,  # Ensure SSL verification
                        allow_redirects=True
                    )
                    resp.raise_for_status()
                    payload = resp.json()

                    # CryptoQuant API response structure
                    if payload.get("status", {}).get("code") == 200:
                        return payload.get("result", {})

                    # API error - log warning dan return None untuk skip
                    error_msg = payload.get("status", {}).get("message", "Unknown error")
                    self.logger.warning(
                        f"API error {endpoint}: {error_msg} - Skipping..."
                    )
                    return None

                except requests.exceptions.Timeout:
                    if attempt < 2:
                        self.logger.warning(f"Request timeout ({timeout}s) {endpoint} - Retrying... ({attempt + 1}/3)")
                        continue
                    else:
                        self.logger.warning(f"Request timeout ({timeout}s) {endpoint} - Skipping after 3 attempts...")
                        return None
                except requests.exceptions.ConnectionError as e:
                    if attempt < 2:
                        self.logger.warning(f"Connection error {endpoint}: {e} - Retrying... ({attempt + 1}/3)")
                        continue
                    else:
                        self.logger.warning(f"Connection failed {endpoint}: {e} - Skipping after 3 attempts...")
                        return None
                except requests.exceptions.RequestException as e:
                    self.logger.warning(f"Request failed {endpoint}: {e} - Skipping...")
                    return None

        except Exception as e:
            self.logger.warning(f"Unexpected error {endpoint}: {e} - Skipping...")
            return None

    def get_exchange_inflow_cdd(self, exchange: str, start_date: str = None, end_date: str = None,
                              interval: str = "day") -> List[Dict[str, Any]]:
        """
        Get Exchange Inflow CDD data from CryptoQuant API

        Args:
            exchange: Exchange name (binance, kraken, bybit, etc.) or aggregation type
                     (all_exchange, spot_exchange, derivative_exchange)
            start_date: Start date in YYYYMMDD format (default: 7 days ago)
            end_date: End date in YYYYMMDD format (default: today)
            interval: Data interval (hour, day, week) - using CryptoQuant parameter names

        Returns:
            List of data points with date and value fields
        """
        # Build endpoint with parameters according to CryptoQuant API
        endpoint = f"btc/flow-indicator/exchange-inflow-cdd"
        params = {
            "exchange": exchange,
            "window": interval
        }

        data = self._make_request(endpoint, params)

        if not data:
            return []

        # Transform data to standardized format
        result = []
        for item in data.get("data", []):
            try:
                result.append({
                    "date": item.get("date"),
                    "value": float(item.get("inflow_cdd", 0))
                })
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error parsing CDD data point: {e}")
                continue

        return result

    