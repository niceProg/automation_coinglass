import requests
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from app.core.logging import setup_logger
from app.core.config import Settings


class CoinglassClient:
    """Coinglass API v4 Client"""

    BASE_URL = "https://open-api-v4.coinglass.com/api"

    def __init__(self):
        self.logger = setup_logger(__name__)
        cfg = Settings()
        self.api_key = cfg.COINGLASS_API_KEY
        if not self.api_key:
            raise ValueError("COINGLASS_API_KEY is required")
        self.headers = {"accept": "application/json", "CG-API-KEY": self.api_key}

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.BASE_URL}/{endpoint}"
        if params:
            params = {k: v for k, v in params.items() if v is not None and v != ""}
            if params:
                url = f"{url}?{urlencode(params)}"

        try:
            self.logger.debug(f"[Coinglass] GET {url}")
            # Timeout 10 detik untuk avoid stuck
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("code") == "0":
                return payload.get("data", None)
            # API error (400, dll) - log warning dan return None untuk skip
            self.logger.warning(
                f"API error {endpoint}: code={payload.get('code')} msg={payload.get('msg')} - Skipping..."
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

    # ---------- Trading Markets ----------
    # DISABLED - Not documented
    # def get_supported_coins(self) -> List[str]:
    #     return self._make_request("futures/supported-coins") or []

    # def get_supported_exchange_pairs(self) -> Dict[str, List[Dict[str, Any]]]:
    #     """Map exchange -> list of {instrument_id, base_asset, quote_asset}"""
    #     return self._make_request("futures/supported-exchange-pairs") or {}

    # DISABLED - Not documented
    # def get_delisted_pairs(self) -> Dict[str, List[Dict[str, Any]]]:
    #     """Map exchange -> list of delisted pairs"""
    #     return self._make_request("futures/delisted-exchange-pairs") or {}

    # DISABLED - Not documented
    # def get_coins_markets(
    #     self,
    #     exchange_list: Optional[str] = None,
    #     per_page: Optional[int] = None,
    #     page: Optional[int] = None,
    # ) -> List[Dict]:
    #     params: Dict[str, Any] = {}
    #     if exchange_list:
    #         params["exchange_list"] = exchange_list
    #     if per_page:
    #         params["per_page"] = per_page
    #     if page:
    #         params["page"] = page
    #     return self._make_request("futures/coins-markets", params) or []

    # def get_pairs_markets(self, symbol: str) -> List[Dict]:
    #     return self._make_request("futures/pairs-markets", {"symbol": symbol}) or []

    # DISABLED - Not documented
    # def get_coins_price_change(self) -> List[Dict]:
    #     return self._make_request("futures/coins-price-change") or []

    # DISABLED - Not documented
    # def get_price_ohlc(
    #     self,
    #     exchange: str,
    #     symbol: str,  # e.g. BTCUSDT
    #     interval: str = "1h",
    #     limit: int = 1000,
    #     start_time: Optional[int] = None,
    #     end_time: Optional[int] = None,
    # ) -> List[Dict]:
    #     params: Dict[str, Any] = {
    #         "exchange": exchange,
    #         "symbol": symbol,
    #         "interval": interval,
    #         "limit": min(int(limit or 1000), 1000),
    #     }
    #     if start_time:
    #         params["start_time"] = start_time
    #     if end_time:
    #         params["end_time"] = end_time
    #     return self._make_request("futures/price/history", params) or []

    # ---------- Open Interest ----------
    
    # DISABLED - Not documented
    # def get_oi_exchange_history_chart(
    #     self, symbol: str, range_: str = "12h", unit: str = None
    # ):
    #     params: Dict[str, Any] = {"symbol": symbol, "range": range_}
    #     if unit:
    #         params["unit"] = unit
    #     return (
    #         self._make_request("futures/open-interest/exchange-history-chart", params)
    #         or {}
    #     )

    def get_oi_aggregated_history(
        self,
        symbol: str,  # Trading coin (e.g., BTC)
        interval: str = "1d",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        unit: str = "usd",
    ) -> List[Dict]:
        """
        Get OHLC Aggregated History for open interest.

        Args:
            symbol: Trading coin (e.g., BTC). Retrieve supported coins via the 'supported-coins' API
            interval: Time interval for data aggregation. Supported values: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w
            start_time: Start timestamp in milliseconds (e.g., 1641522717000)
            end_time: End timestamp in milliseconds (e.g., 1641522717000)
            unit: Unit for the returned data, choose between 'usd' or 'coin'
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "unit": unit,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/open-interest/aggregated-history", params) or []

    # DISABLED - Not documented
    # def get_oi_history(
    #     self,
    #     exchange: str,
    #     symbol: str,  # e.g. BTCUSDT
    #     interval: str = "1h",
    #     limit: int = 1000,
    #     start_time: int = None,
    #     end_time: int = None,
    #     unit: str = None,  # 'usd' | 'coin'
    # ):
    #     params: Dict[str, Any] = {
    #         "exchange": exchange,
    #         "symbol": symbol,
    #         "interval": interval,
    #         "limit": min(int(limit or 1000), 1000),
    #     }
    #     if start_time:
    #         params["start_time"] = start_time
    #     if end_time:
    #         params["end_time"] = end_time
    #     if unit:
    #         params["unit"] = unit
    #     return self._make_request("futures/open-interest/history", params) or []

    # DISABLED - These are now replaced by the original open_interest methods
    # def get_open_interest_exchange_list(self, symbol: str):
    # def get_open_interest_aggregated_history(self, symbol: str, interval: str = "1d", limit: int = 1000, start_time: Optional[int] = None, end_time: Optional[int] = None, unit: str = "usd") -> List[Dict]:

    # ---------- Long / Short Ratio ----------
    def get_lsr_global_account_ratio_history(
        self,
        exchange: str,
        symbol: str,  # pair
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict]:
        """
        Get Global Account Ratio history.

        Args:
            exchange: Exchange name (e.g., Binance, Bybit)
            symbol: Trading pair (e.g., BTCUSDT)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/global-long-short-account-ratio/history", params) or []

    def get_lsr_top_account_ratio_history(
        self,
        exchange: str,
        symbol: str,  # pair
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict]:
        """
        Get Top Account Ratio history.

        Args:
            exchange: Exchange name (e.g., Binance, Bybit)
            symbol: Trading pair (e.g., BTCUSDT)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/top-long-short-account-ratio/history", params) or []

    # ---------- Liquidation ----------
    def get_liquidation_aggregated_history(
        self,
        exchange_list: str,
        symbol: str,  # coin
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict]:
        """
        Get Coin Liquidation Aggregated History.

        Args:
            exchange_list: List of exchange names (e.g., 'Binance,Bybit')
            symbol: Trading coin (e.g., BTC, ETH, SOL)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params = {
            "exchange_list": exchange_list,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/liquidation/aggregated-history", params) or []

    def get_liquidation_aggregated_heatmap(
        self,
        symbol: str,  # coin
        range_param: str = "3d",
    ) -> Dict:
        """
        Get Coin Liquidation Aggregated Heatmap Model3.

        Args:
            symbol: Trading coin (e.g., BTC, ETH, SOL, XRP, HYPE, BNB, DOGE)
            range_param: Time range (12h, 24h, 3d, 7d, 30d, 90d, 180d, 1y)
        """
        params = {
            "symbol": symbol,
            "range": range_param,
        }
        return self._make_request("futures/liquidation/aggregated-heatmap/model3", params) or {}

    # ---------- Futures Basis ----------
    def get_futures_basis_history(
        self,
        exchange: str,
        symbol: str,  # pair
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict]:
        """
        Get Futures Basis History.

        Args:
            exchange: Exchange name (e.g., Binance, Bybit)
            symbol: Trading pair (e.g., BTCUSDT, ETHUSDT)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/basis/history", params) or []

    # def get_lsr_top_position_ratio_history(  # DISABLED
    #     self,
    #     exchange: str,
    #     symbol: str,  # pair
    #     interval: str = "1h",
    #     limit: int = 1000,
    #     start_time: int = None,
    #     end_time: int = None,
    # ):
    #     params = {
    #         "exchange": exchange,
    #         "symbol": symbol,
    #         "interval": interval,
    #         "limit": min(int(limit or 1000), 1000),
    #     }
    #     if start_time:
    #         params["start_time"] = start_time
    #     if end_time:
    #         params["end_time"] = end_time
    #     return (
    #         self._make_request("futures/top-long-short-position-ratio/history", params)
    #         or []
    #     )

    # ---------- Funding Rate ----------
    def get_fr_history(
        self,
        exchange: str,
        symbol: str,  # pair, e.g. BTCUSDT
        interval: str = "1h",
        start_time: int = None,
        end_time: int = None,
    ):
        params = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._make_request("futures/funding-rate/history", params) or []

    def get_fr_exchange_list(self, symbol: str):
        # Response: list of {symbol, stablecoin_margin_list, token_margin_list}
        return (
            self._make_request("futures/funding-rate/exchange-list", {"symbol": symbol})
            or []
        )

    # ========== On-chain / Exchange Data ==========
    # def get_exchange_assets(
    #     self,
    #     exchange: str,
    #     per_page: Optional[int] = None,
    #     page: Optional[int] = None,
    # ) -> List[Dict]:
    #     params: Dict[str, Any] = {"exchange": exchange}
    #     if per_page:
    #         params["per_page"] = str(per_page)
    #     if page:
    #         params["page"] = str(page)
    #     return self._make_request("exchange/assets", params) or []

    # DISABLED - Not documented
    # def get_exchange_balance_list(self, symbol: str) -> List[Dict]:
    #     return self._make_request("exchange/balance/list", {"symbol": symbol}) or []

    # DISABLED - Not documented
    # def get_exchange_balance_chart(self, symbol: str) -> Dict:
    #     data = self._make_request("exchange/balance/chart", {"symbol": symbol}) or []
    #     # API mengembalikan list dengan satu objek; pakai objek pertama bila ada
    #     if isinstance(data, list) and data:
    #         return data[0]
    #     return data or {}

    # ========== On-chain / Transactions ==========
    # def get_exchange_onchain_transfers_erc20(
    #     self,
    #     symbol: Optional[str] = None,  # ERC-20 symbol (optional)
    #     start_time: Optional[int] = None,  # ms
    #     min_usd: Optional[float] = None,
    #     per_page: Optional[int] = None,  # max 100
    #     page: Optional[int] = None,
    # ) -> List[Dict]:
    #     params: Dict[str, Any] = {}
    #     if symbol:
    #         params["symbol"] = symbol
    #     if start_time:
    #         params["start_time"] = int(start_time)
    #     if min_usd is not None:
    #         params["min_usd"] = float(min_usd)
    #     if per_page:
    #         params["per_page"] = int(per_page)
    #     if page:
    #         params["page"] = int(page)
    #     return self._make_request("exchange/chain/tx/list", params) or []

    # ---------- Bitcoin ETF ----------
    def get_btc_etf_list(self) -> List[Dict[str, Any]]:
        return self._make_request("etf/bitcoin/list") or []

    # DISABLED - Not documented
    # def get_btc_hk_etf_flows_history(self) -> List[Dict[str, Any]]:
    #     return self._make_request("hk-etf/bitcoin/flow-history") or []

    # DISABLED - Not documented
    # def get_btc_etf_netassets_history(
    #     self, ticker: Optional[str] = None
    # ) -> List[Dict[str, Any]]:
    #     params: Dict[str, Any] = {}
    #     if ticker:
    #         params["ticker"] = ticker
    #     return self._make_request("etf/bitcoin/net-assets/history", params) or []

    def get_btc_etf_flows_history(self) -> List[Dict[str, Any]]:
        return self._make_request("etf/bitcoin/flow-history") or []

    def get_btc_etf_premium_discount_history(
        self, ticker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if ticker:
            params["ticker"] = ticker
        return self._make_request("etf/bitcoin/premium-discount/history", params) or []

    # DISABLED - Not documented
    # def get_btc_etf_history(self, ticker: str) -> List[Dict[str, Any]]:
    #     return self._make_request("etf/bitcoin/history", {"ticker": ticker}) or []

    # DISABLED - Not documented
    # def get_btc_etf_price_history(
    #     self, ticker: str, range_: str
    # ) -> List[Dict[str, Any]]:
    #     return (
    #         self._make_request(
    #             "etf/bitcoin/price/history", {"ticker": ticker, "range": range_}
    #         )
    #         or []
    #     )

    # DISABLED - Not documented
    # def get_btc_etf_detail(self, ticker: str):
    #     """
    #     Return a single detail dict for the ETF.
    #     Handle both cases where _make_request already unwraps `data` or not.
    #     """
    #     payload = self._make_request("etf/bitcoin/detail", {"ticker": ticker})
    #
    #     # If payload is the wrapper {code, msg, data: {...}}
    #     if isinstance(payload, dict) and "data" in payload:
    #         payload = payload.get("data") or {}
    #
    #     # If somehow it's a list, take the first dict
    #     if isinstance(payload, list):
    #         payload = payload[0] if payload and isinstance(payload[0], dict) else {}
    #
    #     return payload or {}

    def get_etf_bitcoin_list(self) -> List[Dict[str, Any]]:
        """
        Get Bitcoin ETF List - Real-time data for all API plans.
        Returns list of Bitcoin ETFs with key status information.
        """
        return self._make_request("etf/bitcoin/list") or []


    # DISABLED - Endpoint not documented in API markdown
    # def get_etf_history(self, ticker: str) -> List[Dict[str, Any]]:
    #     """
    #     Get ETF History - Historical premium/discount fluctuations for a specific ETF.
    #
    #     Args:
    #         ticker: ETF ticker symbol (e.g., GBTC, IBIT)
    #     """
    #     return self._make_request("etf/bitcoin/history", {"ticker": ticker}) or []

    def get_etf_premium_discount_history(self, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get ETF Premium/Discount History - Historical premium/discount across multiple ETFs.

        Args:
            ticker: ETF ticker symbol (optional, e.g., GBTC, IBIT). If not provided, returns all ETFs.

        Returns:
            Flattened list of records with structure:
            [
                {
                    "timestamp": 1706227200000,
                    "ticker": "GBTC",
                    "nav_usd": 37.51,
                    "market_price_usd": 37.51,
                    "premium_discount_details": 0
                },
                ...
            ]
        """
        params: Dict[str, Any] = {}
        if ticker:
            params["ticker"] = ticker

        data = self._make_request("etf/bitcoin/premium-discount/history", params)
        if not data:
            return []

        # Flatten the nested structure: data is array of {timestamp, list: [...]}
        flattened = []
        for item in data:
            timestamp = item.get("timestamp")
            etf_list = item.get("list", [])

            for etf_record in etf_list:
                flattened.append({
                    "timestamp": timestamp,
                    "ticker": etf_record.get("ticker"),
                    "nav_usd": etf_record.get("nav_usd"),
                    "market_price_usd": etf_record.get("market_price_usd"),
                    "premium_discount_details": etf_record.get("premium_discount_details")
                })

        return flattened

    def get_etf_flows_history(self) -> List[Dict[str, Any]]:
        """
        Get ETF Flows History - Historical flow data for Bitcoin ETFs including daily net inflows/outflows.
        """
        return self._make_request("etf/bitcoin/flow-history") or []


    # ---------- Ethereum ETF ----------
    # DISABLED - Not documented
    # def get_eth_etf_netassets_history(self):
    #     return self._make_request("etf/ethereum/net-assets/history") or []

    # DISABLED - Not documented
    # def get_eth_etf_list(self):
    #     return self._make_request("etf/ethereum/list") or []

    # DISABLED - Not documented
    # def get_eth_etf_flows_history(self):
    #     return self._make_request("etf/ethereum/flow-history") or []


    # ---------- Options (DISABLED) ----------
    # def get_option_max_pain(self, symbol: str, exchange: str = "Deribit") -> List[Dict]:
    #     """
    #     Get option max pain data.
    #
    #     Args:
    #         symbol: Trading coin (e.g., BTC, ETH)
    #         exchange: Exchange name (e.g., Deribit, Binance, OKX)
    #     """
    #     params = {"symbol": symbol, "exchange": exchange}
    #     return self._make_request("option/max-pain", params) or []
    #
    # def get_option_info(self, symbol: str) -> List[Dict]:
    #     """
    #     Get options info.
    #
    #     Args:
    #         symbol: Trading coin (e.g., BTC, ETH)
    #     """
    #     params = {"symbol": symbol}
    #     return self._make_request("option/info", params) or []

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
        return self._make_request("spot/orderbook/ask-bids-history", params) or []

    def get_spot_orderbook_aggregated(
        self,
        exchange_list: str,
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
        return self._make_request("spot/orderbook/aggregated-ask-bids-history", params) or []

    # ---------- Spot Markets ----------
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
        return self._make_request("spot/coins-markets", params) or []

    def get_spot_pairs_markets(
        self,
        symbol: str,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Pairs Markets - Trading pair market data for spot markets.

        Args:
            symbol: Trading symbol (e.g., BTC, ETH)
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
        }
        return self._make_request("spot/pairs-markets", params) or []

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
        exchange_list: str,
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

    # ---------- Macro Overlay ----------
    def get_bitcoin_vs_global_m2_growth(self) -> List[Dict[str, Any]]:
        """
        Get Bitcoin vs Global M2 Supply & Growth.

        Returns historical data comparing Bitcoin price with global M2 money supply.
        """
        return self._make_request("index/bitcoin-vs-global-m2-growth") or []

    # ---------- Options ----------
    def get_option_exchange_oi_history(
        self,
        symbol: str,
        unit: str = "USD",
        range_param: str = "1h",
    ) -> Dict[str, Any]:
        """
        Get Exchange Open Interest History for Options.

        Args:
            symbol: Trading coin (e.g., BTC, ETH)
            unit: Unit for returned data (USD, BTC, ETH depending on symbol)
            range_param: Time range (1h, 4h, 12h, all)
        """
        params = {
            "symbol": symbol,
            "unit": unit,
            "range": range_param,
        }
        return self._make_request("option/exchange-oi-history", params) or {}

    # ---------- Sentiment & On-chain ----------
    def get_fear_greed_history(self) -> Dict[str, Any]:
        """
        Get Crypto Fear & Greed Index history.

        Returns historical fear and greed index data.
        """
        return self._make_request("index/fear-greed-history") or {}

    def get_hyperliquid_whale_alert(self) -> List[Dict[str, Any]]:
        """
        Get Hyperliquid Whale Alert.

        Returns recent large position changes on Hyperliquid exchange.
        """
        return self._make_request("hyperliquid/whale-alert") or []

    def get_chain_whale_transfer(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Whale Transfer on-chain data.

        Returns large on-chain transfers (minimum $10M) within the past six months
        across major blockchains.

        Args:
            symbol: Trading coin (e.g., BTC, ETH, SOL) - optional
            start_time: Start timestamp in milliseconds - optional
            end_time: End timestamp in milliseconds - optional
        """
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("chain/whale-transfer", params) or []

    # ===== NEW ENDPOINTS =====

    def get_futures_footprint_history(
        self,
        exchange: str,
        symbol: str,
        interval: str = "1h",
        limit: int = 1000,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Futures Volume Footprint History - Shows taker buy/sell volumes at different price ranges.

        Args:
            exchange: Exchange name (Binance, OKX, Bybit, Hyperliquid)
            symbol: Trading pair (e.g., BTCUSDT)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            limit: Number of results (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
        }
        if limit:
            params["limit"] = str(limit)
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("futures/volume/footprint-history", params) or []

    def get_spot_large_orderbook_history(
        self,
        exchange: str,
        symbol: str,
        start_time: int,
        end_time: int,
        state: str = "1",
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Large Orderbook History - Historical data for large limit orders.

        Args:
            exchange: Exchange name (e.g., Binance)
            symbol: Trading pair (e.g., BTCUSDT)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            state: Order state (1=In Progress, 2=Finish, 3=Revoke)
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
            "start_time": str(start_time),
            "end_time": str(end_time),
            "state": state,
        }
        return self._make_request("spot/orderbook/large-limit-order-history", params) or []

    def get_spot_large_orderbook(
        self,
        exchange: str,
        symbol: str,
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Large Orderbook - Current large limit orders.

        Args:
            exchange: Exchange name (e.g., Binance)
            symbol: Trading pair (e.g., BTCUSDT)
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
        }
        return self._make_request("spot/orderbook/large-limit-order", params) or []

    def get_spot_aggregated_taker_volume_history(
        self,
        exchange_list: str,
        symbol: str,
        interval: str = "h1",
        limit: int = 1000,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        unit: str = "usd",
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Aggregated Taker Buy/Sell Volume History - Aggregated data across multiple exchanges.

        Args:
            exchange_list: List of exchange names (e.g., "Binance, OKX, Bybit")
            symbol: Trading coin (e.g., BTC)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            limit: Number of results (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            unit: Data unit (usd or coin)
        """
        params: Dict[str, Any] = {
            "exchange_list": exchange_list,
            "symbol": symbol,
            "interval": interval,
            "unit": unit,
        }
        if limit:
            params["limit"] = str(limit)
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("spot/aggregated-taker-buy-sell-volume/history", params) or []

    def get_spot_taker_volume_history(
        self,
        exchange: str,
        symbol: str,
        interval: str = "1h",
        limit: int = 1000,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        unit: str = "usd",
    ) -> List[Dict[str, Any]]:
        """
        Get Spot Taker Buy/Sell Volume History - Single exchange taker volume data.

        Args:
            exchange: Exchange name (e.g., Binance)
            symbol: Trading coin (e.g., BTC)
            interval: Time interval (1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w)
            limit: Number of results (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            unit: Data unit (usd or coin)
        """
        params: Dict[str, Any] = {
            "exchange": exchange,
            "instrument": symbol,  # Try "instrument" instead of "symbol"
            "interval": interval,
            "unit": unit,
        }
        if limit:
            params["limit"] = str(limit)
        if start_time:
            params["start_time"] = str(start_time)
        if end_time:
            params["end_time"] = str(end_time)
        return self._make_request("spot/taker-buy-sell-volume/history", params) or []
