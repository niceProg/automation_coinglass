"""Spot market repository methods for Coinglass data."""

from typing import List, Dict
import pymysql
from app.core.logging import setup_logger


class SpotRepositoryMixin:
    """Mixin class containing spot market repository methods."""

    def __init__(self):
        self.logger = setup_logger(__name__)

    # ===== SPOT ORDERBOOK =====
    def upsert_spot_orderbook_history(self, exchange: str, pair: str, interval: str, range_percent: str, rows: List[Dict]) -> int:
        """Upsert spot orderbook history data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_spot_orderbook_history (
            exchange, pair, `interval`, range_percent, time,
            bids_usd, bids_quantity, asks_usd, asks_quantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            bids_usd=VALUES(bids_usd), bids_quantity=VALUES(bids_quantity),
            asks_usd=VALUES(asks_usd), asks_quantity=VALUES(asks_quantity),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, pair, interval, range_percent, row.get("time"),
                        row.get("bids_usd"), row.get("bids_quantity"),
                        row.get("asks_usd"), row.get("asks_quantity")
                    ))
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_orderbook_history for {exchange}:{pair}:{interval}:{range_percent} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_orderbook_history for {exchange}:{pair}:{interval}:{range_percent} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    def upsert_spot_orderbook_aggregated(self, exchange_list: str, symbol: str, interval: str, range_percent: str, rows: List[Dict]) -> int:
        """Upsert spot orderbook aggregated data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_spot_orderbook_aggregated (
            exchange_list, symbol, `interval`, range_percent, time,
            aggregated_bids_usd, aggregated_bids_quantity,
            aggregated_asks_usd, aggregated_asks_quantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            aggregated_bids_usd=VALUES(aggregated_bids_usd), aggregated_bids_quantity=VALUES(aggregated_bids_quantity),
            aggregated_asks_usd=VALUES(aggregated_asks_usd), aggregated_asks_quantity=VALUES(aggregated_asks_quantity),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange_list, symbol, interval, range_percent, row.get("time"),
                        row.get("aggregated_bids_usd"), row.get("aggregated_bids_quantity"),
                        row.get("aggregated_asks_usd"), row.get("aggregated_asks_quantity")
                    ))
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_orderbook_aggregated for {exchange_list}:{symbol}:{interval}:{range_percent} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_orderbook_aggregated for {exchange_list}:{symbol}:{interval}:{range_percent} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    # ===== SPOT COINS MARKETS =====
    def upsert_spot_coins_markets(self, rows: List[Dict]) -> int:
        """Upsert spot coins markets data."""
        if not rows:
            return 0

        # Build the SQL dynamically to avoid format string issues
        columns = [
            "symbol", "current_price", "market_cap",
            "price_change_5m", "price_change_15m", "price_change_30m",
            "price_change_1h", "price_change_4h", "price_change_12h",
            "price_change_24h", "price_change_1w",
            "price_change_percent_5m", "price_change_percent_15m", "price_change_percent_30m",
            "price_change_percent_1h", "price_change_percent_4h", "price_change_percent_12h",
            "price_change_percent_24h", "price_change_percent_1w",
            "volume_usd_1h", "volume_usd_5m", "volume_usd_15m", "volume_usd_30m",
            "volume_usd_4h", "volume_usd_12h", "volume_usd_24h", "volume_usd_1w",
            "volume_change_usd_1h", "volume_change_usd_5m", "volume_change_usd_15m",
            "volume_change_usd_30m", "volume_change_usd_4h", "volume_change_usd_12h",
            "volume_change_usd_24h", "volume_change_usd_1w",
            "volume_change_percent_1h", "volume_change_percent_5m", "volume_change_percent_15m",
            "volume_change_percent_30m", "volume_change_percent_4h", "volume_change_percent_12h",
            "volume_change_percent_24h", "volume_change_percent_1w",
            "buy_volume_usd_1h", "buy_volume_usd_5m", "buy_volume_usd_15m",
            "buy_volume_usd_30m", "buy_volume_usd_4h", "buy_volume_usd_12h",
            "buy_volume_usd_24h", "buy_volume_usd_1w",
            "sell_volume_usd_1h", "sell_volume_usd_5m", "sell_volume_usd_15m",
            "sell_volume_usd_30m", "sell_volume_usd_4h", "sell_volume_usd_12h",
            "sell_volume_usd_24h", "sell_volume_usd_1w",
            "volume_flow_usd_1h", "volume_flow_usd_5m", "volume_flow_usd_15m",
            "volume_flow_usd_30m", "volume_flow_usd_4h", "volume_flow_usd_12h",
            "volume_flow_usd_24h", "volume_flow_usd_1w"
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

        sql = f"""
        INSERT INTO cg_spot_coins_markets ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {update_clauses},
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    values = [row.get(col) for col in columns]
                    cur.execute(sql, values)
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_coins_markets - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_coins_markets - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    # ===== SPOT PAIRS MARKETS =====
    def upsert_spot_pairs_markets(self, rows: List[Dict]) -> int:
        """Upsert spot pairs markets data."""
        if not rows:
            return 0

        # Build the SQL dynamically to avoid format string issues
        columns = [
            "symbol", "exchange_name", "current_price",
            "price_change_1h", "price_change_percent_1h",
            "volume_usd_1h", "buy_volume_usd_1h", "sell_volume_usd_1h",
            "volume_change_usd_1h", "volume_change_percent_1h", "net_flows_usd_1h",
            "price_change_4h", "price_change_percent_4h",
            "volume_usd_4h", "buy_volume_usd_4h", "sell_volume_usd_4h",
            "volume_change_4h", "volume_change_percent_4h", "net_flows_usd_4h",
            "price_change_12h", "price_change_percent_12h",
            "volume_usd_12h", "buy_volume_usd_12h", "sell_volume_usd_12h",
            "volume_change_12h", "volume_change_percent_12h", "net_flows_usd_12h",
            "price_change_24h", "price_change_percent_24h",
            "volume_usd_24h", "buy_volume_usd_24h", "sell_volume_usd_24h",
            "volume_change_24h", "volume_change_percent_24h", "net_flows_usd_24h",
            "price_change_1w", "price_change_percent_1w",
            "volume_usd_1w", "buy_volume_usd_1w", "sell_volume_usd_1w",
            "volume_change_usd_1w", "volume_change_percent_1w", "net_flows_usd_1w"
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

        sql = f"""
        INSERT INTO cg_spot_pairs_markets ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {update_clauses},
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    values = [row.get(col) for col in columns]
                    cur.execute(sql, values)
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_pairs_markets - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_pairs_markets - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    # ===== SPOT PRICE HISTORY =====
    def upsert_spot_price_history(self, exchange: str, symbol: str, interval: str, rows: List[Dict]) -> int:
        """Upsert spot price history data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_spot_price_history (
            exchange, symbol, `interval`, time, open, high, low, close, volume_usd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low),
            close=VALUES(close), volume_usd=VALUES(volume_usd),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, symbol, interval, row.get("time"),
                        row.get("open"), row.get("high"),
                        row.get("low"), row.get("close"),
                        row.get("volume_usd")
                    ))
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_price_history for {exchange}:{symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_price_history for {exchange}:{symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    # ===== OPEN INTEREST AGGREGATED STABLECOIN HISTORY =====
    def upsert_open_interest_aggregated_stablecoin_history(self, exchange_list: str, symbol: str, interval: str, rows: List[Dict]) -> int:
        """Upsert open interest aggregated stablecoin history (OHLC) data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_open_interest_aggregated_stablecoin_history (
            exchange_list, symbol, `interval`, time, open, high, low, close
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange_list, symbol, interval, row.get("time"),
                        row.get("open"), row.get("high"),
                        row.get("low"), row.get("close")
                    ))
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting open_interest_aggregated_stablecoin_history for {exchange_list}:{symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting open_interest_aggregated_stablecoin_history for {exchange_list}:{symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    # ===== EXCHANGE RANK =====
    def upsert_exchange_rank(self, rows: List[Dict]) -> int:
        """Upsert exchange rank data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_exchange_rank (
            exchange_name, symbol, open_interest_usd, open_interest_rank,
            volume_usd_24h, volume_rank, liquidation_usd_24h, liquidation_rank, create_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_interest_usd=VALUES(open_interest_usd), open_interest_rank=VALUES(open_interest_rank),
            volume_usd_24h=VALUES(volume_usd_24h), volume_rank=VALUES(volume_rank),
            liquidation_usd_24h=VALUES(liquidation_usd_24h), liquidation_rank=VALUES(liquidation_rank),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        row.get("exchange_name"), row.get("symbol"),
                        row.get("open_interest_usd"), row.get("open_interest_rank"),
                        row.get("volume_usd_24h"), row.get("volume_rank"),
                        row.get("liquidation_usd_24h"), row.get("liquidation_rank"),
                        row.get("create_time")
                    ))
            self.conn.commit()
            return len(rows)
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting exchange_rank - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting exchange_rank - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0