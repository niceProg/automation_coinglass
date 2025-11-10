# app/repositories/cryptoquant_repository.py
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from app.models.cryptoquant import CRYPTOQUANT_TABLES
from app.database.connection import get_connection


class CryptoQuantRepository:
    """Repository for CryptoQuant data operations"""

    def __init__(self, connection=None, logger=None):
        self.conn = connection or get_connection()
        self.logger = logger or logging.getLogger(__name__)

    def _execute_query(self, query: str, params: tuple = None) -> Any:
        """Execute a query and return results"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                if query.strip().upper().startswith("SELECT"):
                    return cur.fetchall()
                elif query.strip().upper().startswith("INSERT"):
                    return cur.lastrowid, cur.rowcount
                else:
                    return cur.rowcount
        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            raise
        finally:
            self.conn.commit()

    def upsert_exchange_inflow_cdd(self, exchange: str, date: str, interval: str,
                                 value: float) -> Dict[str, int]:
        """
        Upsert exchange inflow CDD data

        Returns:
            Dict with keys: 'exchange_inflow_cdd', 'exchange_inflow_cdd_duplicates'
        """
        result = {
            "exchange_inflow_cdd": 0,
            "exchange_inflow_cdd_duplicates": 0
        }

        query = """
        INSERT INTO cq_exchange_inflow_cdd (exchange, date, `interval`, value)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        value = VALUES(value),
        updated_at = CURRENT_TIMESTAMP
        """

        try:
            affected = self._execute_query(query, (exchange, date, interval, value))

            # affected will be 1 for new insert, 2 for update
            if affected == 1:
                result["exchange_inflow_cdd"] += 1
            else:
                result["exchange_inflow_cdd_duplicates"] += 1

        except Exception as e:
            self.logger.error(f"Error upserting exchange inflow CDD: {e}")
            raise

        return result

    def upsert_exchange_inflow_cdd_batch(self, exchange: str, interval: str,
                                        data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upsert multiple exchange inflow CDD records

        Args:
            exchange: Exchange name
            interval: Data interval
            data: List of dictionaries with 'date' and 'value' keys

        Returns:
            Dict with keys: 'exchange_inflow_cdd', 'exchange_inflow_cdd_duplicates'
        """
        result = {
            "exchange_inflow_cdd": 0,
            "exchange_inflow_cdd_duplicates": 0
        }

        if not data:
            return result

        query = """
        INSERT INTO cq_exchange_inflow_cdd (exchange, date, `interval`, value)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        value = VALUES(value),
        updated_at = CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for item in data:
                    date = item.get("date")
                    value = item.get("value")

                    if date is not None and value is not None:
                        cur.execute(query, (exchange, date, interval, value))

                        # Check if this was an insert or update
                        if cur.rowcount == 1:
                            result["exchange_inflow_cdd"] += 1
                        else:
                            result["exchange_inflow_cdd_duplicates"] += 1

                self.conn.commit()

        except Exception as e:
            self.logger.error(f"Error upserting exchange inflow CDD batch: {e}")
            raise

        return result

    def upsert_btc_market_price(self, date: str, open_price: float, high_price: float,
                              low_price: float, close_price: float) -> Dict[str, int]:
        """
        Upsert Bitcoin market price data

        Returns:
            Dict with keys: 'btc_market_price', 'btc_market_price_duplicates'
        """
        result = {
            "btc_market_price": 0,
            "btc_market_price_duplicates": 0
        }

        query = """
        INSERT INTO cq_btc_market_price (date, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        high = VALUES(high),
        low = VALUES(low),
        close = VALUES(close),
        updated_at = CURRENT_TIMESTAMP
        """

        try:
            affected = self._execute_query(query, (date, open_price, high_price, low_price, close_price))

            # affected will be 1 for new insert, 2 for update
            if affected == 1:
                result["btc_market_price"] += 1
            else:
                result["btc_market_price_duplicates"] += 1

        except Exception as e:
            self.logger.error(f"Error upserting BTC market price: {e}")
            raise

        return result

    def upsert_btc_market_price_batch(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upsert multiple Bitcoin market price records

        Args:
            data: List of dictionaries with 'date', 'open', 'high', 'low', 'close' keys

        Returns:
            Dict with keys: 'btc_market_price', 'btc_market_price_duplicates'
        """
        result = {
            "btc_market_price": 0,
            "btc_market_price_duplicates": 0
        }

        if not data:
            return result

        query = """
        INSERT INTO cq_btc_market_price (date, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        high = VALUES(high),
        low = VALUES(low),
        close = VALUES(close),
        updated_at = CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for item in data:
                    date = item.get("date")
                    open_price = item.get("open")
                    high_price = item.get("high")
                    low_price = item.get("low")
                    close_price = item.get("close")

                    if date is not None and close_price is not None:
                        cur.execute(query, (date, open_price, high_price, low_price, close_price))

                        # Check if this was an insert or update
                        if cur.rowcount == 1:
                            result["btc_market_price"] += 1
                        else:
                            result["btc_market_price_duplicates"] += 1

                self.conn.commit()

        except Exception as e:
            self.logger.error(f"Error upserting BTC market price batch: {e}")
            raise

        return result

    def get_exchange_inflow_cdd_data(self, exchange: str, start_date: str = None,
                                   end_date: str = None, interval: str = None,
                                   limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve exchange inflow CDD data from database

        Returns:
            List of dictionaries with CDD data
        """
        conditions = []
        params = []

        if exchange:
            conditions.append("exchange = %s")
            params.append(exchange)
        if start_date:
            conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("date <= %s")
            params.append(end_date)
        if interval:
            conditions.append("interval = %s")
            params.append(interval)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT exchange, date, interval, value, created_at, updated_at
        FROM cq_exchange_inflow_cdd
        WHERE {where_clause}
        ORDER BY date DESC, exchange
        LIMIT %s
        """
        params.append(limit)

        try:
            results = self._execute_query(query, tuple(params))

            columns = ['exchange', 'date', 'interval', 'value', 'created_at', 'updated_at']
            return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            self.logger.error(f"Error retrieving exchange inflow CDD data: {e}")
            return []

    def get_btc_market_price_data(self, start_date: str = None, end_date: str = None,
                                limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve Bitcoin market price data from database

        Returns:
            List of dictionaries with price data
        """
        conditions = []
        params = []

        if start_date:
            conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("date <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT date, open, high, low, close, created_at, updated_at
        FROM cq_btc_market_price
        WHERE {where_clause}
        ORDER BY date DESC
        LIMIT %s
        """
        params.append(limit)

        try:
            results = self._execute_query(query, tuple(params))

            columns = ['date', 'open', 'high', 'low', 'close', 'created_at', 'updated_at']
            return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            self.logger.error(f"Error retrieving BTC market price data: {e}")
            return []