"""Spot market client methods for Coinglass API."""

from typing import Dict, Any, List, Optional


class SpotClientMixin:
    """Mixin class containing spot market client methods."""

    # ---------- Spot Orderbook ----------
    def get_spot_orderbook_history(
        self,
        exchange: str,
        pair: str,
        interval: str = "1m",
        range_percent: str = "0.25",
        limit: int = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Orderbook History - Historical orderbook data for spot trading.

        Args:
            exchange: Exchange name (e.g., Binance, OKX)
            pair: Trading pair (e.g., BTCUSDT)
            interval: Data aggregation interval
            range_percent: Price range percentage (e.g., "0.25", "0.5")
            limit: Number of results (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "symbol": pair,
            "interval": interval,
            "range": range_percent,
        }
        if limit:
            params["limit"] = str(limit)
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("spot/orderbook/history", params) or []

    def get_spot_orderbook_aggregated(
        self,
        exchange_list: str,  # Comma-separated list of exchanges
        symbol: str,
        interval: str = "1m",
        range_percent: str = "0.25",
        limit: int = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Orderbook Aggregated - Aggregated orderbook data across multiple exchanges.

        Args:
            exchange_list: Comma-separated exchange names (e.g., "Binance,OKX")
            symbol: Trading symbol (e.g., BTC)
            interval: Data aggregation interval
            range_percent: Price range percentage
            limit: Number of results (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params: Dict[str, Any] = {
            "exchange_list": exchange_list,
            "symbol": symbol,
            "interval": interval,
            "range": range_percent,
        }
        if limit:
            params["limit"] = str(limit)
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("spot/orderbook/aggregated", params) or []

    # ---------- Spot Markets ----------
    def get_spot_supported_exchange_pairs(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get Spot Supported Exchange Pairs - Reference data for supported spot trading pairs.

        Returns:
            Dictionary mapping exchange names to list of supported pairs
        """
        return self._make_request("spot/supported-exchange-pairs") or {}

    def get_spot_coins_markets(
        self,
        symbols: Optional[List[str]] = None,
        per_page: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Coins Markets - Comprehensive spot market data for multiple coins.

        Args:
            symbols: List of coin symbols to fetch
            per_page: Number of results per page (max 100)
            page: Page number
        """
        params: Dict[str, Any] = {
            "per_page": str(per_page),
            "page": str(page),
        }
        if symbols:
            params["symbols"] = ",".join(symbols)
        return self._make_request("spot/coins/markets", params) or []

    def get_spot_pairs_markets(
        self,
        symbols: Optional[List[str]] = None,
        per_page: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Pairs Markets - Trading pair market data for spot markets.

        Args:
            symbols: List of trading symbols to fetch
            per_page: Number of results per page (max 100)
            page: Page number
        """
        params: Dict[str, Any] = {
            "per_page": str(per_page),
            "page": str(page),
        }
        if symbols:
            params["symbols"] = ",".join(symbols)
        return self._make_request("spot/pairs/markets", params) or []

    def get_spot_price_history(
        self,
        exchange: str,
        symbol: str,
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Price OHLC History - Historical OHLC price data for spot trading.

        Args:
            exchange: Exchange name (e.g., Binance, OKX)
            symbol: Trading pair (e.g., BTCUSDT)
            interval: Data aggregation time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("spot/price/history", params) or []

    # ---------- Open Interest Aggregated Stablecoin History ----------
    def get_open_interest_aggregated_stablecoin_history(
        self,
        exchange_list: str,  # Comma-separated list of exchanges
        symbol: str,
        interval: str = "1d",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get OHLC Aggregated Stablecoin Margin History - OHLC data for aggregated stablecoin margin open interest.

        Args:
            exchange_list: Comma-separated exchange names (e.g., "Binance,OKX,Bybit")
            symbol: Trading coin (e.g., BTC)
            interval: Time interval for data aggregation (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params: Dict[str, Any] = {
            "exchange_list": exchange_list,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("futures/open-interest/aggregated-stablecoin-history", params) or []

    # ---------- Exchange Rank ----------
    def get_exchange_rank(
        self,
        symbol: str,
    ) -> List[Dict[str, Any]]:
        """
        Get Exchange Rank - Exchange rankings by open interest, volume, and liquidations.

        Args:
            symbol: Trading symbol (e.g., BTC)
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
        }
        return self._make_request("futures/exchange-rank", params) or []