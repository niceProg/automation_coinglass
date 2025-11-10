# app/services/cryptoquant_service.py
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from app.database.connection import get_connection
from app.providers.cryptoquant.client import CryptoQuantClient
from app.providers.cryptoquant.pipelines import (
    exchange_inflow_cdd,
    btc_market_price,
)

logger = logging.getLogger(__name__)


class CryptoQuantService:
    """Service for managing CryptoQuant data collection pipelines"""

    def __init__(self):
        self.logger = logger
        self.client = None
        self.conn = None

    def _initialize(self):
        """Initialize database connection and API client"""
        if not self.conn:
            self.conn = get_connection()
        if not self.client:
            self.client = CryptoQuantClient()

    def _close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def run_pipeline(self, pipeline_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run a specific CryptoQuant pipeline

        Args:
            pipeline_name: Name of the pipeline to run
            params: Optional parameters for the pipeline

        Returns:
            Dict with pipeline execution results
        """
        self._initialize()

        if params is None:
            params = {}

        try:
            # Map pipeline names to pipeline functions
            pipelines = {
                "exchange_inflow_cdd": exchange_inflow_cdd,
                "btc_market_price": btc_market_price,
            }

            if pipeline_name not in pipelines:
                raise ValueError(f"Unknown pipeline: {pipeline_name}")

            pipeline_func = pipelines[pipeline_name]
            logger.info(f"Running pipeline '{pipeline_name}'")

            result = pipeline_func.run(self.conn, self.client, params)

            if isinstance(result, dict):
                logger.info(f"Pipeline '{pipeline_name}' completed successfully")
                logger.info(f"Result: {result}")
            else:
                logger.info(f"Pipeline '{pipeline_name}' completed with result: {result}")

            return result

        except Exception as e:
            logger.error(f"Error running pipeline '{pipeline_name}': {e}")
            raise
        finally:
            self._close()

    def run_all_pipelines(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run all available CryptoQuant pipelines

        Args:
            params: Optional parameters for all pipelines

        Returns:
            Dict with combined results from all pipelines
        """
        self._initialize()

        if params is None:
            params = {}

        pipelines = ["exchange_inflow_cdd", "btc_market_price"]
        results = {}

        try:
            for pipeline_name in pipelines:
                logger.info(f"Running pipeline '{pipeline_name}'")
                try:
                    result = self.run_pipeline(pipeline_name, params)
                    results[pipeline_name] = result
                except Exception as e:
                    logger.error(f"Pipeline '{pipeline_name}' failed: {e}")
                    results[pipeline_name] = {"error": str(e)}

            return results

        except Exception as e:
            logger.error(f"Error running all pipelines: {e}")
            raise
        finally:
            self._close()

    def get_exchange_inflow_cdd_data(self, exchange: str = None, start_date: str = None,
                                    end_date: str = None, interval: str = None,
                                    limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get Exchange Inflow CDD data from database

        Args:
            exchange: Exchange name
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            interval: Data interval
            limit: Maximum number of records to return

        Returns:
            List of dictionaries with CDD data
        """
        self._initialize()

        try:
            from app.repositories.cryptoquant_repository import CryptoQuantRepository
            repo = CryptoQuantRepository(self.conn, self.logger)

            return repo.get_exchange_inflow_cdd_data(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                limit=limit
            )
        finally:
            self._close()

    def get_btc_market_price_data(self, start_date: str = None, end_date: str = None,
                                limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get Bitcoin market price data from database

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of records to return

        Returns:
            List of dictionaries with price data
        """
        self._initialize()

        try:
            from app.repositories.cryptoquant_repository import CryptoQuantRepository
            repo = CryptoQuantRepository(self.conn, self.logger)

            return repo.get_btc_market_price_data(
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
        finally:
            self._close()

    def get_available_exchanges(self) -> List[str]:
        """
        Get list of available exchanges for CDD data

        Returns:
            List of exchange names
        """
        return [
            "all_exchange",
            "spot_exchange",
            "derivative_exchange",
            "binance",
            "kraken",
            "bybit",
            "gemini",
            "bitfinex",
            "kucoin",
            "bitstamp",
            "mexc"
        ]

    def get_available_intervals(self) -> List[str]:
        """
        Get list of available intervals

        Returns:
            List of interval names
        """
        return ["1h", "4h", "1d", "1w"]