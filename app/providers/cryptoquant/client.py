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
            # Timeout 10 detik untuk avoid stuck
            resp = requests.get(url, headers=self.headers, timeout=10)
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
            self.logger.warning(f"Request timeout (10s) {endpoint} - Skipping...")
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
        if exchange in ["all_exchange", "spot_exchange", "derivative_exchange"]:
            # Aggregated exchanges - use 'exchanges' parameter
            endpoint = f"btc/flow-indicator/exchange-inflow-cdd"
            params = {
                "exchanges": exchange,
                "window": interval
            }
        else:
            # Single exchange - use 'exchange' parameter
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

    def get_btc_market_price(self, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        Get Bitcoin market price data from CryptoQuant API

        Note: This endpoint is not currently available in the CryptoQuant API or requires
        a different endpoint path. This method is structured for future implementation
        when the correct endpoint is identified.

        Args:
            start_date: Start date in YYYYMMDD format (default: 7 days ago)
            end_date: End date in YYYYMMDD format (default: today) - not used in API call

        Returns:
            List of price data points with OHLC data (empty list until endpoint is available)
        """
        self.logger.warning("BTC market price endpoint is not available in current CryptoQuant API")
        self.logger.info("The price endpoint may require a different API path or higher subscription tier")

        # Try multiple potential endpoints
        potential_endpoints = [
            "btc/market-data/price",
            "btc/price",
            "btc/market-price",
            "btc/flow-indicator/price"
        ]

        for endpoint in potential_endpoints:
            try:
                # Use window parameter without limit
                params = {
                    "window": "day"
                }

                data = self._make_request(endpoint, params)

                if data and data.get("data"):
                    self.logger.info(f"Successfully found price data at endpoint: {endpoint}")

                    # Transform data to standardized format
                    result = []
                    for item in data.get("data", []):
                        try:
                            result.append({
                                "date": item.get("date"),
                                "open": float(item.get("open", 0)),
                                "high": float(item.get("high", 0)),
                                "low": float(item.get("low", 0)),
                                "close": float(item.get("close", 0))
                            })
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"Error parsing price data point: {e}")
                            continue

                    return result

            except Exception as e:
                self.logger.debug(f"Endpoint {endpoint} failed: {e}")
                continue

        self.logger.warning("No working BTC price endpoint found. Consider using external price data source.")
        return []