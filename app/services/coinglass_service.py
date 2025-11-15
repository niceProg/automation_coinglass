# app/services/coinglass_service.py
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from app.database.connection import get_connection
from app.providers.coinglass.client import CoinglassClient
from app.providers.coinglass.pipelines import (
    funding_rate,
    # open_interest,  # DISABLED - Replaced by separate OI pipelines
    # oi_history,  # DISABLED
    oi_aggregated_history,
    long_short_ratio_global,
    long_short_ratio_top,
    liquidation_aggregated,
    liquidation_heatmap,
    futures_basis,
    # options,  # OPTIONS DISABLED
    # exchange_assets,  # DISABLED
    # exchange_balance_list,  # DISABLED - Not documented
    # exchange_onchain_transfers,  # DISABLED
    spot_coins_markets,
    spot_pairs_markets,
    spot_price_history,
    # ===== NEW ENDPOINTS =====
    futures_footprint_history,
    spot_large_orderbook_history,
    spot_large_orderbook,
    spot_aggregated_taker_volume_history,
    spot_taker_volume_history,
    bitcoin_etf_list,
    # bitcoin_etf_history,  # DISABLED - Endpoint not documented in API markdown
    bitcoin_etf_flows_history,
    bitcoin_etf_premium_discount_history,
    # Trading Market Pipelines
    # supported_exchange_pairs,  # DISABLED
    # pairs_markets,  # DISABLED
    # coins_markets,  # DISABLED
    # Macro Overlay Pipelines
    bitcoin_vs_global_m2_growth,
    # Options Pipelines
    option_exchange_oi_history,
    # open_interest_exchange_list,  # DISABLED - Table deleted
    # Open Interest Aggregated Stablecoin History Pipeline
    open_interest_aggregated_stablecoin_history,
        # Sentiment Pipelines
    fear_greed_index,
    hyperliquid_whale_alert,
    whale_transfer,
    # ===== ASK BIDS ENDPOINTS =====
    spot_ask_bids_history,
    spot_aggregated_ask_bids_history,
)
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import settings
from app.monitoring.freshness_monitor import DataFreshnessMonitor

logger = logging.getLogger(__name__)


class CoinglassService:
    """Service to orchestrate Coinglass data ingestion."""

    def __init__(self, ensure_tables: bool = False):
        self.conn = get_connection()
        if not self.conn:
            raise ConnectionError("Failed to connect to database")

        self.client = CoinglassClient()

        # Default parameters for pipelines
        self.default_params = {
            "symbols": settings.COINGLASS_SYMBOLS,
            "exchanges": ["Binance", "Bybit"],
            # "limit": 1000,  # Removed - using API default
            "min_usd": settings.MIN_USD,
        }

        # Pipeline mapping
        self.pipelines = {
            "funding_rate": {
                "func": funding_rate.run,
                "params": {
                    "symbols": settings.COINGLASS_SYMBOLS,
                    "exchanges": ["Binance", "Bybit"],
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    # "limit": 1000,  # Removed - using API default
                    "min_usd": settings.MIN_USD,
                },
            },
            # OI History Pipeline
            # "oi_history": {  # DISABLED
            #     "func": oi_history.run,
            #     "params": {
            #         "symbols": settings.COINGLASS_SYMBOLS,
            #         "exchanges": ["Binance", "Bybit"],
            #         "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
            #         "unit": "usd",
            #     },
            # },
            # OI Aggregated History Pipeline
            "oi_aggregated_history": {
                "func": oi_aggregated_history.run,
                "params": {
                    "symbols": settings.COINGLASS_SYMBOLS,
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "unit": "usd",
                },
            },
            "long_short_ratio_global": {
                "func": long_short_ratio_global.run,
                "params": {
                    "symbols": settings.COINGLASS_SYMBOLS,
                    "exchanges": ["Binance", "Bybit"],
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "min_usd": settings.MIN_USD,
                },
            },
            "long_short_ratio_top": {
                "func": long_short_ratio_top.run,
                "params": {
                    "symbols": settings.COINGLASS_SYMBOLS,
                    "exchanges": ["Binance", "Bybit"],
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "min_usd": settings.MIN_USD,
                },
            },
            "liquidation_aggregated": {
                "func": liquidation_aggregated.run,
                "params": {
                    "symbols": ["BTC", "ETH", "SOL"],
                    "exchange_list": "Binance,Bybit",
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "min_usd": settings.MIN_USD,
                },
            },
            "liquidation_heatmap": {
                "func": liquidation_heatmap.run,
                "params": {
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "ranges": ["12h", "24h", "3d", "7d", "30d", "90d", "180d", "1y"],
                    "min_usd": settings.MIN_USD,
                },
            },
            "futures_basis": {
                "func": futures_basis.run,
                "params": {
                    "pairs": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"],
                    "exchanges": ["Binance", "Bybit"],
                    "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "min_usd": settings.MIN_USD,
                },
            },
            # "options": {  # OPTIONS DISABLED
            #     "func": options.run,
            #     "params": {
            #         "symbols": settings.COINGLASS_SYMBOLS,
            #         "exchanges": ["OKX", "Binance", "Bybit", "Deribit"],
            #         # "limit": 1000,  # Removed - using API default
            #         "min_usd": settings.MIN_USD,
            #     },
            # },
            # "exchange_assets": {
            #     "func": exchange_assets.run,
            #     "params": {**self.default_params, "per_page": 10, "page": 1},
            # },
            # "exchange_balance_list": {  # DISABLED - Not documented
            #     "func": exchange_balance_list.run,
            #     "params": self.default_params,
            # },
            # "exchange_onchain_transfers": {
            #     "func": exchange_onchain_transfers.run,
            #     "params": {"symbols": ["USDT", "USDC", "SHIB", "MANA", "LINK", "AAVE"], "hours_back": 24, "per_page": 100, "page": 1, "min_usd": 100},  # Only requested tokens
            # },
            # "spot_orderbook": {  # DISABLED
            #     "func": spot_orderbook.run,
            #     "params": {
            #         "symbols": ["BTCUSDT"],
            #         "exchanges": ["Binance", "Bybit"],
            #         "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
            #         "ranges": ["0.25", "0.5"],
            #         # "limit": 100,  # Removed - using API default
            #         "hours_back": 2,
            #         "min_usd": settings.MIN_USD,
            #     },
            # },
            # "spot_orderbook_aggregated": {  # DISABLED
            #     "func": spot_orderbook_aggregated.run,
            #     "params": {
            #         "symbols": ["BTC"],
            #         "exchange_lists": ["Binance,Bybit"],
            #         "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
            #         "ranges": ["0.25", "0.5"],
            #         # "limit": 100,  # Removed - using API default
            #         "hours_back": 2,
            #     },
            # },
            # Spot Market Pipelines with mandatory requirements
            # "spot_coins_markets": {
            #     "func": spot_coins_markets.run,
            #     "params": {"symbols": ["BTC"], "per_page": 100, "page": 1},
            # },
            # "spot_pairs_markets": {
            #     "func": spot_pairs_markets.run,
            #     "params": {**self.default_params, "symbols": ["BTC"]},
            # },
            "spot_price_history": {
                "func": spot_price_history.run,
                "params": {**self.default_params, "symbols": ["BTCUSDT"], "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"], "hours_back": 2},
            },
            # Spot Market Pipelines
            "spot_coins_markets": {
                "func": spot_coins_markets.run,
                "params": {
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "per_page": 100,
                    "page": 1,
                },
            },
            "spot_pairs_markets": {
                "func": spot_pairs_markets.run,
                "params": {**self.default_params, "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"]},
            },
            # Bitcoin ETF Pipelines
            "bitcoin_etf_list": {
                "func": bitcoin_etf_list.run,
                "params": {},  # Real-time data, no specific params needed
            },
            # "bitcoin_etf_history": {  # DISABLED - Endpoint not documented in API markdown
            #     "func": bitcoin_etf_history.run,
            #     "params": {"tickers": ["GBTC", "IBIT", "FBTC", "ARKB", "BITO", "BRRR"]},  # Major Bitcoin ETFs
            # },
            "bitcoin_etf_flows_history": {
                "func": bitcoin_etf_flows_history.run,
                "params": {},  # All ETF flows history
            },
            "bitcoin_etf_premium_discount_history": {
                "func": bitcoin_etf_premium_discount_history.run,
                "params": {},  # All ETFs premium/discount history
            },
            # Trading Market Pipelines
            # "supported_exchange_pairs": {
            #     "func": supported_exchange_pairs.run,
            #     "params": {},  # Reference data, no specific params needed
            # },
            # "pairs_markets": {
            #     "func": pairs_markets.run,
            #     "params": {**self.default_params, "symbols": ["BTC"]},
            # },
            # "coins_markets": {  # DISABLED
            #     "func": coins_markets.run,
            #     "params": {"exchange_list": "Binance,Bybit", "per_page": 50, "page": 1},
            # },
            # Macro Overlay Pipelines
            "bitcoin_vs_global_m2_growth": {
                "func": bitcoin_vs_global_m2_growth.run,
                "params": {},  # No params required
            },
            # Options Pipelines
            "option_exchange_oi_history": {
                "func": option_exchange_oi_history.run,
                "params": {
                    "symbols": ["BTC", "ETH"],
                    "units": ["USD"],
                    "ranges": ["1h", "4h", "12h", "all"],
                },
            },
            # Open Interest Exchange List (DISABLED - Use original open_interest pipeline instead)
            # "open_interest_exchange_list": {
            #     "func": open_interest_exchange_list.run,
            #     "params": {
            #         "symbols": settings.COINGLASS_SYMBOLS,
            #     },
            # },
            # Open Interest Aggregated Stablecoin History Pipeline
            "open_interest_aggregated_stablecoin_history": {
                "func": open_interest_aggregated_stablecoin_history.run,
                "params": {
                    "exchange_lists": ["Binance,Bybit"],
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                },
            },
            # Sentiment Pipelines
            "fear_greed_index": {
                "func": fear_greed_index.run,
                "params": {},  # No params required
            },
            "hyperliquid_whale_alert": {
                "func": hyperliquid_whale_alert.run,
                "params": {},  # No params required
            },
            "whale_transfer": {
                "func": whale_transfer.run,
                "params": {
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "DOGE"],
                },
            },
            # ===== NEW ENDPOINTS =====
            "futures_footprint_history": {
                "func": futures_footprint_history.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "limit": 1000,
                    "hours_back": 24,
                },
            },
            "spot_large_orderbook_history": {
                "func": spot_large_orderbook_history.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"],
                    "states": ["1", "2", "3"],  # All states: In Progress, Finish, Revoke
                    "hours_back": 24,
                },
            },
            "spot_large_orderbook": {
                "func": spot_large_orderbook.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"],
                },
            },
            "spot_aggregated_taker_volume_history": {
                "func": spot_aggregated_taker_volume_history.run,
                "params": {
                    "exchange_lists": ["Binance", "Binance,Bybit"],
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "limit": 1000,
                    "unit": "usd",
                    "hours_back": 24,
                },
            },
            "spot_taker_volume_history": {
                "func": spot_taker_volume_history.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "limit": 1000,
                    "unit": "usd",
                    "hours_back": 24,
                },
            },
            # ===== ASK BIDS ENDPOINTS =====
            "spot_ask_bids_history": {
                "func": spot_ask_bids_history.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "ranges": ["0.25", "0.5"],
                    "hours_back": 24,
                },
            },
            "spot_aggregated_ask_bids_history": {
                "func": spot_aggregated_ask_bids_history.run,
                "params": {
                    "exchanges": ["Binance", "Bybit"],
                    "symbols": ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"],
                    "intervals": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    "ranges": ["0.25", "0.5"],
                    "hours_back": 24,
                },
            },
        }

        # Initialize freshness monitor
        self.freshness_monitor = DataFreshnessMonitor(self.conn)

        # Only ensure tables exist if explicitly requested
        if ensure_tables:
            repository = CoinglassRepository(self.conn, logger)
            repository.ensure_schema()

    def ensure_tables(self):
        """Ensure database tables exist."""
        repository = CoinglassRepository(self.conn, logger)
        repository.ensure_schema()

    def run_pipeline(
        self, pipeline_name: str, custom_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Run a single pipeline with optional custom parameters."""
        if pipeline_name not in self.pipelines:
            raise ValueError(
                f"Unknown pipeline: {pipeline_name}. "
                f"Available: {list(self.pipelines.keys())}"
            )

        pipeline_config = self.pipelines[pipeline_name]
        pipeline_func = pipeline_config["func"]

        # Merge default params with custom params
        params = pipeline_config["params"].copy()
        if custom_params:
            params.update(custom_params)

        # Apply exchange filtering from environment variable
        exchange_filter = os.getenv("EXCHANGE_FILTER")
        if exchange_filter:
            # Filter exchanges parameter if it exists
            if "exchanges" in params:
                filtered = [exchange_filter]
                logger.info(f"Applying exchange filter: {exchange_filter}")
                params["exchanges"] = filtered

            # Filter exchange_list parameter for coins_markets
            if "exchange_list" in params:
                # Parse the existing exchange_list and filter to only the specified exchange
                existing_exchanges = params["exchange_list"].split(",")
                if exchange_filter in existing_exchanges:
                    params["exchange_list"] = exchange_filter
                    logger.info(f"Applying exchange_list filter: {exchange_filter}")
                else:
                    logger.warning(f"Exchange filter '{exchange_filter}' not in exchange_list, skipping pipeline")
                    return {"skipped": f"Exchange {exchange_filter} not configured for this pipeline"}

        logger.info(f"Running pipeline '{pipeline_name}'{f' for exchange {exchange_filter}' if exchange_filter else ''}")

        try:
            result = pipeline_func(self.conn, self.client, params)
            logger.info(f"Pipeline '{pipeline_name}' completed successfully")
            return result
        except Exception as e:
            logger.error(f"Pipeline '{pipeline_name}' failed: {e}", exc_info=True)
            return {"error": str(e)}

    def run_selected_pipelines(self, pipeline_names: List[str]) -> Dict[str, Any]:
        """Run selected pipelines."""
        results = {}
        for name in pipeline_names:
            try:
                results[name] = self.run_pipeline(name)
            except Exception as e:
                logger.error(f"Failed to run pipeline {name}: {e}")
                results[name] = {"error": str(e)}
        return results

    def run_all_pipelines(self, check_freshness: bool = True) -> Dict[str, Any]:
        """Run all pipelines with optional freshness monitoring."""
        logger.info("Running all pipelines...")

        # Run pipelines
        results = self.run_selected_pipelines(list(self.pipelines.keys()))

        # Check freshness after running pipelines
        if check_freshness:
            self.check_and_log_freshness()

        return results

    def run_initial_scrape(self, months: int = 1) -> Dict[str, Any]:
        """Run initial historical data scrape for N months."""
        logger.info(f"Running initial scrape for {months} month(s)")

        # Calculate start_time (N months ago)
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(days=30 * months)).timestamp() * 1000)

        # Custom params for initial scrape
        initial_params = {
            "start_time": start_time,
            "end_time": end_time,
            # "limit": 1000,  # Removed - using API default
        }

        results = {}
        for pipeline_name in self.pipelines.keys():
            try:
                logger.info(f"Initial scrape for {pipeline_name}...")
                results[pipeline_name] = self.run_pipeline(pipeline_name, initial_params)
            except Exception as e:
                logger.error(f"Initial scrape failed for {pipeline_name}: {e}")
                results[pipeline_name] = {"error": str(e)}

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get ingestion status from database."""
        try:
            with self.conn.cursor() as cur:
                status = {}

                # Check each table for latest data
                tables = [
                    ("funding_rate_history", "cg_funding_rate_history"),
                    # ("oi_history", "cg_open_interest_history"),  # DISABLED
                    ("oi_aggregated_history", "cg_open_interest_aggregated_history"),  # ACTIVE in oi_aggregated_history pipeline
                    ("lsr_global_account", "cg_long_short_global_account_ratio_history"),
                    ("lsr_top_account", "cg_long_short_top_account_ratio_history"),
                    ("liquidation_aggregated", "cg_liquidation_aggregated_history"),
                    ("liquidation_heatmap", "cg_liquidation_heatmap"),
                    ("futures_basis", "cg_futures_basis_history"),
                    # ("exchange_assets", "cg_exchange_assets"),  # DISABLED
                    # ("exchange_balance_list", "cg_exchange_balance_list"),  # DISABLED - Not documented
                    # ("exchange_onchain_transfers", "cg_exchange_onchain_transfers"),  # DISABLED
                    # ===== NEW ENDPOINTS =====
                    ("futures_footprint_history", "cg_futures_footprint_history"),
                    ("spot_large_orderbook_history", "cg_spot_large_orderbook_history"),
                    ("spot_large_orderbook", "cg_spot_large_orderbook"),
                    ("spot_aggregated_taker_volume_history", "cg_spot_aggregated_taker_volume_history"),
                    ("spot_taker_volume_history", "cg_spot_taker_volume_history"),
                    # Spot Market Tables
                    ("spot_coins_markets", "cg_spot_coins_markets"),
                    ("spot_pairs_markets", "cg_spot_pairs_markets"),
                    ("spot_price_history", "cg_spot_price_history"),
                    # Bitcoin ETF Tables
                    ("bitcoin_etf_list", "cg_bitcoin_etf_list"),
                    # ("bitcoin_etf_history", "cg_bitcoin_etf_history"),  # DISABLED - Endpoint not documented in API markdown
                    ("bitcoin_etf_flows_history", "cg_bitcoin_etf_flows_history"),
                    ("bitcoin_etf_flows_details", "cg_bitcoin_etf_flows_details"),
                    ("bitcoin_etf_premium_discount_history", "cg_bitcoin_etf_premium_discount_history"),
                ]

                for key, table in tables:
                    try:
                        # Different tables have different time columns, so handle them separately
                        if table == "cg_bitcoin_etf_list":
                            cur.execute(f"SELECT COUNT(*) as count, MAX(update_timestamp) as latest_time FROM {table}")
                        # elif table == "cg_bitcoin_etf_history":  # DISABLED - Endpoint not documented in API markdown
                        #     cur.execute(f"SELECT COUNT(*) as count, MAX(assets_date) as latest_time FROM {table}")
                        elif table == "cg_bitcoin_etf_flows_history":
                            cur.execute(f"SELECT COUNT(*) as count, MAX(timestamp) as latest_time FROM {table}")
                        elif table == "cg_bitcoin_etf_flows_details":
                            cur.execute(f"SELECT COUNT(*) as count, MAX(timestamp) as latest_time FROM {table}")
                        elif table == "cg_bitcoin_etf_premium_discount_history":
                            cur.execute(f"SELECT COUNT(*) as count, MAX(timestamp) as latest_time FROM {table}")
                        # elif table == "cg_exchange_onchain_transfers":
                        #     cur.execute(f"SELECT COUNT(*) as count, MAX(transaction_time) as latest_time FROM {table}")
                        # elif table == "cg_exchange_balance_list":  # DISABLED - Not documented
                        #     # These tables don't have a time column, use updated_at instead
                        #     cur.execute(f"SELECT COUNT(*) as count, MAX(UNIX_TIMESTAMP(updated_at) * 1000) as latest_time FROM {table}")
                        elif table in ["cg_spot_coins_markets", "cg_spot_pairs_markets"]:
                            # These tables don't have a time column, use updated_at instead
                            cur.execute(f"SELECT COUNT(*) as count, MAX(UNIX_TIMESTAMP(updated_at) * 1000) as latest_time FROM {table}")
                        elif table == "cg_spot_large_orderbook_history":
                            # Uses start_time as the primary time column
                            cur.execute(f"SELECT COUNT(*) as count, MAX(start_time) as latest_time FROM {table}")
                        elif table == "cg_spot_large_orderbook":
                            # Uses current_time as the primary time column
                            cur.execute(f"SELECT COUNT(*) as count, MAX(current_time) as latest_time FROM {table}")
                        else:
                            cur.execute(f"SELECT COUNT(*) as count, MAX(time) as latest_time FROM {table}")

                        row = cur.fetchone()
                        status[key] = {
                            "count": row["count"],
                            "latest_time": row["latest_time"],
                            "latest_datetime": datetime.fromtimestamp(row["latest_time"] / 1000).isoformat()
                            if row["latest_time"]
                            else None,
                        }
                    except Exception as e:
                        # If query fails, log the error and provide fallback status
                        logger.warning(f"Failed to get status for {table}: {e}")
                        status[key] = {
                            "count": 0,
                            "latest_time": None,
                            "latest_datetime": None,
                            "error": str(e)
                        }

                return status
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            return {"error": str(e)}

    def check_and_log_freshness(self) -> Dict[str, Any]:
        """Check and log freshness for all data streams."""
        try:
            logger.info("🔍 Checking data freshness...")
            results = self.freshness_monitor.check_all_streams_freshness()
            self.freshness_monitor.log_freshness_status(results)

            # Generate and log alerts
            alerts = self.freshness_monitor.get_freshness_alerts(results)
            if alerts:
                logger.warning("🚨 FRESHNESS ALERTS:")
                for alert in alerts:
                    logger.warning(f"   {alert}")

            return results
        except Exception as e:
            logger.error(f"Failed to check freshness: {e}", exc_info=True)
            return {"error": str(e)}

    def get_freshness_status(self) -> Dict[str, Any]:
        """Get detailed freshness status for all data streams."""
        try:
            return self.freshness_monitor.check_all_streams_freshness()
        except Exception as e:
            logger.error(f"Failed to get freshness status: {e}", exc_info=True)
            return {"error": str(e)}

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
