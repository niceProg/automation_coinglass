# app/repositories/coinglass_repository.py
import logging
import pymysql
from typing import Any, Dict, List, Optional
from app.models.coinglass import COINGLASS_TABLES
import time

logger = logging.getLogger(__name__)


class CoinglassRepository:
    def __init__(self, conn, logger_=None):
        self.conn = conn
        self.logger = logger_ or logger

    def check_existing_records(self, table_name: str, unique_keys: List[Dict]) -> Dict[str, bool]:
        """Check which records already exist in the database."""
        if not unique_keys:
            return {}

        existing_records = set()

        # Build WHERE clause for checking existing records
        placeholders = []
        values = []

        # Get column names from the first key dict
        first_key = unique_keys[0]
        columns = list(first_key.keys())

        for key_dict in unique_keys:
            # Create a composite key for each unique combination
            key_parts = []
            key_values = []
            for column, value in key_dict.items():
                # Properly quote column names that might be reserved words
                quoted_column = f"`{column}`" if column in ['time', 'interval'] else column
                key_parts.append(f"{quoted_column} = %s")
                key_values.append(value)

            placeholders.append(f"({' AND '.join(key_parts)})")
            values.extend(key_values)

        # Get table name and primary key columns
        where_clause = " OR ".join(placeholders)

        # Build CONCAT_WS with properly quoted column names
        quoted_columns = []
        for col in columns:
            quoted_col = f"`{col}`" if col in ['time', 'interval'] else col
            quoted_columns.append(quoted_col)

        # Query to check existing records
        sql = f"""
        SELECT CONCAT_WS('|', {', '.join(quoted_columns)}) as record_key
        FROM {table_name}
        WHERE {where_clause}
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, values)
                results = cur.fetchall()
                existing_records = {row[0] for row in results}

        except Exception as e:
            self.logger.error(f"Error checking existing records in {table_name}: {e}")
            # Assume all records are new if check fails
            return {"|".join(str(v) for v in key_dict.values()): False for key_dict in unique_keys}

        # Return dict indicating which records exist
        result = {}
        for key_dict in unique_keys:
            record_key = '|'.join(str(v) for v in key_dict.values())
            result[record_key] = record_key in existing_records

        return result

    def count_duplicates_in_batch(self, table_name: str, unique_keys: List[Dict]) -> int:
        """Count how many records in a batch are duplicates."""
        if not unique_keys:
            return 0

        existing_checks = self.check_existing_records(table_name, unique_keys)
        duplicate_count = sum(1 for exists in existing_checks.values() if exists)

        return duplicate_count

    def ensure_schema(self):
        """Create all tables."""
        try:
            with self.conn.cursor() as cur:
                for table_name, create_sql in COINGLASS_TABLES.items():
                    cur.execute(create_sql)
                    self.logger.info(f"Table ensured: {table_name}")
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to ensure schema: {e}")
            raise

    # ===== FUNDING RATE =====
    def upsert_fr_history(self, exchange: str, pair: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert funding rate history with duplicate detection."""
        result = {
            "fr_history": 0,
            "fr_history_duplicates": 0,
            "fr_history_filtered": 0
        }

        if not rows:
            return result

        # Filter out rows where open, high, low, and close are 0 or 0.00000000
        filtered_rows = []
        for row in rows:
            open_val = float(row.get("open", 0))
            high_val = float(row.get("high", 0))
            low_val = float(row.get("low", 0))
            close_val = float(row.get("close", 0))

            # Skip if all OHLC values are 0 or 0.00000000
            if open_val == 0 and high_val == 0 and low_val == 0 and close_val == 0:
                self.logger.debug(f"Skipping row with all zero values: {row}")
                continue

            filtered_rows.append(row)

        # Log filtered rows
        filtered_count = len(rows) - len(filtered_rows)
        result["fr_history_filtered"] = filtered_count
        if filtered_count > 0:
            self.logger.info(f"Filtered out {filtered_count} rows with zero values from {len(rows)} total rows for {exchange}:{pair}:{interval}")

        if not filtered_rows:
            self.logger.info(f"No valid rows after filtering for {exchange}:{pair}:{interval}")
            return result

        # Perform the upsert and use ROW_COUNT() to detect inserts vs updates
        total_inserted = 0
        total_updated = 0

        sql = """
        INSERT INTO cg_funding_rate_history (exchange, pair, `interval`, time, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in filtered_rows:
                    cur.execute(sql, (
                        exchange, pair, interval, row.get("time"),
                        row.get("open"), row.get("high"),
                        row.get("low"), row.get("close")
                    ))

                    # Get affected rows count (1 = insert, 2 = update)
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1

            self.conn.commit()

            # Set the results
            result["fr_history"] = total_inserted  # Fresh records (inserted)
            result["fr_history_duplicates"] = total_updated  # Duplicate records (updated)

            if total_updated > 0:
                self.logger.info(f"Inserted {total_inserted} fresh records, updated {total_updated} existing records for {exchange}:{pair}:{interval}")

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting fr_history for {exchange}:{pair}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {"fr_history": 0, "fr_history_duplicates": 0, "fr_history_filtered": filtered_count}
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting fr_history for {exchange}:{pair}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {"fr_history": 0, "fr_history_duplicates": 0, "fr_history_filtered": filtered_count}

    def upsert_fr_exchange_list(self, symbol: str, data: Dict) -> int:
        """Upsert funding rate exchange list."""
        saved = 0
        sql = """
        INSERT INTO cg_funding_rate_exchange_list
            (symbol, exchange, margin_type, funding_rate, funding_rate_interval, next_funding_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            funding_rate=VALUES(funding_rate),
            funding_rate_interval=VALUES(funding_rate_interval),
            next_funding_time=VALUES(next_funding_time),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                # Process stablecoin margin
                for item in data.get("stablecoin_margin_list", []):
                    cur.execute(sql, (
                        symbol, item.get("exchange"), "stablecoin",
                        item.get("funding_rate"), item.get("funding_rate_interval"),
                        item.get("next_funding_time")
                    ))
                    saved += 1

                # Process token margin
                for item in data.get("token_margin_list", []):
                    cur.execute(sql, (
                        symbol, item.get("exchange"), "coin",
                        item.get("funding_rate"), item.get("funding_rate_interval"),
                        item.get("next_funding_time")
                    ))
                    saved += 1

            self.conn.commit()
            return saved
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting fr_exchange_list for {symbol} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return 0
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting fr_exchange_list for {symbol} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return 0

    def upsert_oi_aggregated_history(self, symbol: str, interval: str, rows: List[Dict], unit: str = "usd") -> Dict[str, int]:
        """Upsert open interest aggregated history."""
        result = {
            "oi_aggregated_history": 0,
            "oi_aggregated_history_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_open_interest_aggregated_history (symbol, `interval`, time, open, high, low, close, unit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
            unit=VALUES(unit), updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                total_processed = len(rows)
                for row in rows:
                    cur.execute(sql, (
                        symbol, interval, row.get("time"),
                        row.get("open"), row.get("high"),
                        row.get("low"), row.get("close"), unit
                    ))
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["oi_aggregated_history"] += 1
                    else:
                        result["oi_aggregated_history_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting oi_aggregated_history for {symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting oi_aggregated_history for {symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== DISABLED METHODS (Commented Out) =====

    # ===== OPEN INTEREST (DISABLED) =====
    # def upsert_oi_history(self, exchange: str, pair: str, interval: str, rows: List[Dict], unit: str = "usd") -> int:
    #     """Upsert open interest history."""
    #     if not rows:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_open_interest_history (exchange, pair, `interval`, time, open, high, low, close, unit)
    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
    #         unit=VALUES(unit), updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     exchange, pair, interval, row.get("time"),
    #                     row.get("open"), row.get("high"),
    #                     row.get("low"), row.get("close"), unit
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting oi_history for {exchange}:{pair}:{interval} - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting oi_history for {exchange}:{pair}:{interval} - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    
    # ===== EXCHANGE INFRASTRUCTURE (DISABLED) =====
    # def upsert_exchange_balance_list(self, symbol: str, rows: List[Dict]) -> int:
    #     """Upsert exchange balance list data."""
    #     if not rows:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_exchange_balance_list (
    #         exchange_name, total_balance, balance_change_1d, balance_change_percent_1d,
    #         balance_change_7d, balance_change_percent_7d, balance_change_30d,
    #         balance_change_percent_30d, symbol
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         total_balance=VALUES(total_balance),
    #         balance_change_1d=VALUES(balance_change_1d),
    #         balance_change_percent_1d=VALUES(balance_change_percent_1d),
    #         balance_change_7d=VALUES(balance_change_7d),
    #         balance_change_percent_7d=VALUES(balance_change_percent_7d),
    #         balance_change_30d=VALUES(balance_change_30d),
    #         balance_change_percent_30d=VALUES(balance_change_percent_30d),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     row.get("exchange_name"),
    #                     row.get("total_balance"),
    #                     row.get("balance_change_1d"),
    #                     row.get("balance_change_percent_1d"),
    #                     row.get("balance_change_7d"),
    #                     row.get("balance_change_percent_7d"),
    #                     row.get("balance_change_30d"),
    #                     row.get("balance_change_percent_30d"),
    #                     symbol
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting exchange_balance_list for {symbol} - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting exchange_balance_list for {symbol} - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    # ===== TRADING MARKET (DISABLED) =====
    # def upsert_coins_markets(self, rows: List[Dict]) -> int:
    #     """Upsert coins markets data."""
    #     if not rows:
    #         return 0

    #     # Build SQL dynamically to handle all fields
    #     columns = [
    #         "symbol", "current_price", "avg_funding_rate_by_oi", "avg_funding_rate_by_vol",
    #         "market_cap_usd", "open_interest_market_cap_ratio", "open_interest_usd",
    #         "open_interest_quantity", "open_interest_volume_ratio",
    #         "price_change_percent_5m", "price_change_percent_15m", "price_change_percent_30m",
    #         "price_change_percent_1h", "price_change_percent_4h", "price_change_percent_12h",
    #         "price_change_percent_24h",
    #         "open_interest_change_percent_5m", "open_interest_change_percent_15m",
    #         "open_interest_change_percent_30m", "open_interest_change_percent_1h",
    #         "open_interest_change_percent_4h", "open_interest_change_percent_24h",
    #         "volume_change_percent_5m", "volume_change_percent_15m", "volume_change_percent_30m",
    #         "volume_change_percent_1h", "volume_change_percent_4h", "volume_change_percent_24h",
    #         "volume_change_usd_1h", "volume_change_usd_4h", "volume_change_usd_24h",
    #         "long_short_ratio_5m", "long_short_ratio_15m", "long_short_ratio_30m",
    #         "long_short_ratio_1h", "long_short_ratio_4h", "long_short_ratio_12h",
    #         "long_short_ratio_24h",
    #         "liquidation_usd_1h", "long_liquidation_usd_1h", "short_liquidation_usd_1h",
    #         "liquidation_usd_4h", "long_liquidation_usd_4h", "short_liquidation_usd_4h",
    #         "liquidation_usd_12h", "long_liquidation_usd_12h", "short_liquidation_usd_12h",
    #         "liquidation_usd_24h", "long_liquidation_usd_24h", "short_liquidation_usd_24h"
    #     ]

    #     placeholders = ", ".join(["%s"] * len(columns))
    #     update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

    #     sql = f"""
    #     INSERT INTO cg_coins_markets ({', '.join(columns)})
    #     VALUES ({placeholders})
    #     ON DUPLICATE KEY UPDATE
    #         {update_clauses},
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 values = [row.get(col) for col in columns]
    #                 cur.execute(sql, values)
    #         self.conn.commit()
    #         return len(rows)
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting coins_markets - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting coins_markets - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    # ===== BITCOIN ETF (DISABLED - Endpoint not documented) =====
    # def upsert_bitcoin_etf_history(self, ticker: str, rows: List[Dict]) -> Dict[str, int]:
    #     """Upsert Bitcoin ETF history data with duplicate detection."""
    #     result = {
    #         "bitcoin_etf_history": 0,
    #         "bitcoin_etf_history_duplicates": 0
    #     }

    #     if not rows:
    #         return result

    #     sql = """
    #     INSERT INTO cg_bitcoin_etf_history (
    #         ticker, name, assets_date, btc_holdings, market_date, market_price,
    #         nav, net_assets, premium_discount, shares_outstanding
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         name=VALUES(name), btc_holdings=VALUES(btc_holdings),
    #         market_date=VALUES(market_date), market_price=VALUES(market_price),
    #         nav=VALUES(nav), net_assets=VALUES(net_assets),
    #         premium_discount=VALUES(premium_discount),
    #         shares_outstanding=VALUES(shares_outstanding),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     total_inserted = 0
    #     total_updated = 0

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     ticker,
    #                     row.get("name"),
    #                     row.get("assets_date"),
    #                     row.get("btc_holdings"),
    #                     row.get("market_date"),
    #                     row.get("market_price"),
    #                     row.get("nav"),
    #                     row.get("net_assets"),
    #                     row.get("premium_discount"),
    #                     row.get("shares_outstanding")
    #                 ))

    #                 # Get affected rows count (1 = insert, 2 = update)
    #                 affected_rows = cur.rowcount
    #                 if affected_rows == 1:
    #                     total_inserted += 1
    #                 elif affected_rows == 2:
    #                     total_updated += 1

    #         self.conn.commit()

    #         # Set the results
    #         result["bitcoin_etf_history"] = total_inserted  # Fresh records (inserted)
    #         result["bitcoin_etf_history_duplicates"] = total_updated  # Duplicate records (updated)

    #         if total_updated > 0:
    #             self.logger.info(f"Inserted {total_inserted} fresh ETF history records, updated {total_updated} existing records for {ticker}")

    #         return result

    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting bitcoin_etf_history for {ticker} - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return {"bitcoin_etf_history": 0, "bitcoin_etf_history_duplicates": 0}
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting bitcoin_etf_history for {ticker} - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return {"bitcoin_etf_history": 0, "bitcoin_etf_history_duplicates": 0}

    # ===== ACTIVE METHODS BELOW =====

    # ===== LONG/SHORT RATIO =====
    def upsert_lsr_global_account_ratio(self, exchange: str, pair: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert global long/short account ratio."""
        result = {
            "lsr_global_account_ratio": 0,
            "lsr_global_account_ratio_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_long_short_global_account_ratio_history (
            exchange, pair, `interval`, time, global_account_long_percent,
            global_account_short_percent, global_account_long_short_ratio
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            global_account_long_percent=VALUES(global_account_long_percent),
            global_account_short_percent=VALUES(global_account_short_percent),
            global_account_long_short_ratio=VALUES(global_account_long_short_ratio),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, pair, interval, row.get("time"),
                        row.get("global_account_long_percent"),
                        row.get("global_account_short_percent"),
                        row.get("global_account_long_short_ratio")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["lsr_global_account_ratio"] += 1
                    elif cur.rowcount == 2:
                        result["lsr_global_account_ratio_duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error upserting lsr_global_account_ratio: {e}")
            return result

    def upsert_lsr_top_account_ratio(self, exchange: str, pair: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """
        Upsert Top Account Long/Short Ratio history.

        Endpoint: /api/futures/top-long-short-account-ratio/history
        Table: cg_long_short_top_account_ratio_history
        """
        result = {
            "lsr_top_account_ratio": 0,
            "lsr_top_account_ratio_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_long_short_top_account_ratio_history (
            exchange, pair, `interval`, time, top_account_long_percent,
            top_account_short_percent, top_account_long_short_ratio
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            top_account_long_percent=VALUES(top_account_long_percent),
            top_account_short_percent=VALUES(top_account_short_percent),
            top_account_long_short_ratio=VALUES(top_account_long_short_ratio),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, pair, interval, row.get("time"),
                        row.get("top_account_long_percent"),
                        row.get("top_account_short_percent"),
                        row.get("top_account_long_short_ratio")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["lsr_top_account_ratio"] += 1
                    elif cur.rowcount == 2:
                        result["lsr_top_account_ratio_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting lsr_top_account_ratio [{exchange}:{pair}:{interval}] - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting lsr_top_account_ratio [{exchange}:{pair}:{interval}] - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== LIQUIDATION =====
    def upsert_liquidation_aggregated_history(self, symbol: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert liquidation aggregated history with duplicate detection."""
        result = {
            "liquidation_aggregated": 0,
            "liquidation_aggregated_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_liquidation_aggregated_history (
            symbol, `interval`, time, aggregated_long_liquidation_usd, aggregated_short_liquidation_usd
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            aggregated_long_liquidation_usd=VALUES(aggregated_long_liquidation_usd),
            aggregated_short_liquidation_usd=VALUES(aggregated_short_liquidation_usd),
            updated_at=CURRENT_TIMESTAMP
        """

        total_inserted = 0
        total_updated = 0
        skipped_count = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    # Get liquidation values
                    long_liq = row.get("aggregated_long_liquidation_usd")
                    short_liq = row.get("aggregated_short_liquidation_usd")

                    # Skip if both values are None or 0
                    if (long_liq is None or long_liq == 0) and (short_liq is None or short_liq == 0):
                        skipped_count += 1
                        continue

                    cur.execute(sql, (
                        symbol, interval, row.get("time"),
                        long_liq,
                        short_liq
                    ))

                    # Get affected rows count (1 = insert, 2 = update)
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1

            self.conn.commit()

            # Set the results
            result["liquidation_aggregated"] = total_inserted  # Fresh records (inserted)
            result["liquidation_aggregated_duplicates"] = total_updated  # Duplicate records (updated)

            if total_updated > 0 or skipped_count > 0:
                self.logger.info(
                    f"Liquidation Aggregated [{symbol}:{interval}]: "
                    f"Inserted {total_inserted} fresh records, "
                    f"updated {total_updated} existing records, "
                    f"skipped {skipped_count} zero-value records"
                )

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting liquidation_aggregated_history for {symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {"liquidation_aggregated": 0, "liquidation_aggregated_duplicates": 0}
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting liquidation_aggregated_history for {symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {"liquidation_aggregated": 0, "liquidation_aggregated_duplicates": 0}

    def upsert_liquidation_heatmap(self, symbol: str, range_param: str, data: Dict) -> Dict[str, int]:
        """Upsert liquidation heatmap data with relational structure."""
        result = {
            "liquidation_heatmap": 0,
            "liquidation_heatmap_duplicates": 0
        }

        if not data:
            return result

        try:
            with self.conn.cursor() as cur:
                # Step 1: Insert or update main record
                main_sql = """
                INSERT INTO cg_liquidation_heatmap (symbol, `range`)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE updated_at=CURRENT_TIMESTAMP
                """
                cur.execute(main_sql, (symbol, range_param))

                # Get affected rows count (1 = insert, 2 = update)
                affected_rows = cur.rowcount
                is_update = (affected_rows == 2)

                if affected_rows == 1:
                    result["liquidation_heatmap"] = 1
                elif affected_rows == 2:
                    result["liquidation_heatmap_duplicates"] = 1

                # Step 2: Get the liquidation_heatmap_id
                cur.execute(
                    "SELECT id FROM cg_liquidation_heatmap WHERE symbol=%s AND `range`=%s",
                    (symbol, range_param)
                )
                row = cur.fetchone()
                liquidation_heatmap_id = row['id']

                # Step 3: Delete existing child records (for updates)
                if is_update:
                    cur.execute(
                        "DELETE FROM cg_liquidation_heatmap_y_axis WHERE liquidation_heatmap_id=%s",
                        (liquidation_heatmap_id,)
                    )
                    cur.execute(
                        "DELETE FROM cg_liquidation_heatmap_leverage_data WHERE liquidation_heatmap_id=%s",
                        (liquidation_heatmap_id,),
                    )
                    cur.execute(
                        "DELETE FROM cg_liquidation_heatmap_price_candlesticks WHERE liquidation_heatmap_id=%s",
                        (liquidation_heatmap_id,)
                    )

                # Step 4: Insert y_axis data
                y_axis_data = data.get("y_axis", [])
                if y_axis_data:
                    y_axis_sql = """
                    INSERT INTO cg_liquidation_heatmap_y_axis
                    (liquidation_heatmap_id, price_level, sequence_order)
                    VALUES (%s, %s, %s)
                    """
                    y_axis_values = [
                        (liquidation_heatmap_id, float(price_level), idx)
                        for idx, price_level in enumerate(y_axis_data)
                    ]
                    cur.executemany(y_axis_sql, y_axis_values)

                # Step 5: Insert liquidation_leverage_data
                liquidation_leverage_data = data.get("liquidation_leverage_data", [])
                if liquidation_leverage_data:
                    lev_sql = """
                    INSERT INTO cg_liquidation_heatmap_leverage_data
                    (liquidation_heatmap_id, sequence_order, x_position, y_position, liquidation_amount)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    lev_values = []
                    for idx, item in enumerate(liquidation_leverage_data):
                        if isinstance(item, (list, tuple)) and len(item) >= 3:
                            lev_values.append((
                                liquidation_heatmap_id, idx,
                                int(item[0]) if item[0] is not None else None,
                                int(item[1]) if item[1] is not None else None,
                                float(item[2]) if item[2] is not None else None
                            ))
                    if lev_values:
                        cur.executemany(lev_sql, lev_values)

                # Step 6: Insert price_candlesticks data (OHLCV format)
                price_candlesticks = data.get("price_candlesticks", [])
                if price_candlesticks:
                    candle_sql = """
                    INSERT INTO cg_liquidation_heatmap_price_candlesticks
                    (liquidation_heatmap_id, sequence_order, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    candle_values = []
                    for idx, item in enumerate(price_candlesticks):
                        # price_candlesticks format: [timestamp, open, high, low, close, volume]
                        if isinstance(item, (list, tuple)) and len(item) >= 6:
                            candle_values.append((
                                liquidation_heatmap_id, idx,
                                int(item[0]) if item[0] is not None else None,  # timestamp
                                float(item[1]) if item[1] is not None else None,  # open
                                float(item[2]) if item[2] is not None else None,  # high
                                float(item[3]) if item[3] is not None else None,  # low
                                float(item[4]) if item[4] is not None else None,  # close
                                float(item[5]) if item[5] is not None else None   # volume
                            ))
                    if candle_values:
                        cur.executemany(candle_sql, candle_values)

            self.conn.commit()

            if result["liquidation_heatmap_duplicates"] > 0:
                self.logger.info(f"Updated existing heatmap record for {symbol}:{range_param}")

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting liquidation_heatmap for {symbol}:{range_param} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {"liquidation_heatmap": 0, "liquidation_heatmap_duplicates": 0}
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting liquidation_heatmap for {symbol}:{range_param} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {"liquidation_heatmap": 0, "liquidation_heatmap_duplicates": 0}

    # ===== FUTURES BASIS =====
    def upsert_futures_basis_history(self, exchange: str, pair: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert futures basis history."""
        result = {
            "futures_basis": 0,
            "futures_basis_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_futures_basis_history (
            exchange, pair, `interval`, time, open_basis, close_basis, open_change, close_change
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_basis=VALUES(open_basis),
            close_basis=VALUES(close_basis),
            open_change=VALUES(open_change),
            close_change=VALUES(close_change),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, pair, interval, row.get("time"),
                        row.get("open_basis"),
                        row.get("close_basis"),
                        row.get("open_change"),
                        row.get("close_change")
                    ))
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["futures_basis"] += 1
                    else:
                        result["futures_basis_duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error upserting futures_basis_history: {e}")
            return result

    # def upsert_lsr_position_ratio(self, exchange: str, pair: str, interval: str, rows: List[Dict]) -> int:
    #     """Upsert long/short position ratio."""
    #     if not rows:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_long_short_position_ratio_history (
    #         exchange, pair, `interval`, time, top_position_long_percent,
    #         top_position_short_percent, top_position_long_short_ratio
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         top_position_long_percent=VALUES(top_position_long_percent),
    #         top_position_short_percent=VALUES(top_position_short_percent),
    #         top_position_long_short_ratio=VALUES(top_position_long_short_ratio),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     exchange, pair, interval, row.get("time"),
    #                     row.get("top_position_long_percent"),
    #                     row.get("top_position_short_percent"),
    #                     row.get("top_position_long_short_ratio")
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting lsr_position_ratio: {e}")
    #         return 0

    # ===== OPTIONS (DISABLED) =====
    # def upsert_option_max_pain(self, symbol: str, exchange: str, rows: List[Dict]) -> int:
    #         """Upsert option max pain data."""
    #         if not rows:
    #             return 0
    #
    #         sql = """
    #         INSERT INTO cg_option_max_pain (
    #             symbol, exchange, date, call_open_interest_market_value, put_open_interest,
    #             put_open_interest_market_value, max_pain_price, call_open_interest,
    #             call_open_interest_notional, put_open_interest_notional
    #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         ON DUPLICATE KEY UPDATE
    #             call_open_interest_market_value=VALUES(call_open_interest_market_value),
    #             put_open_interest=VALUES(put_open_interest),
    #             put_open_interest_market_value=VALUES(put_open_interest_market_value),
    #             max_pain_price=VALUES(max_pain_price),
    #             call_open_interest=VALUES(call_open_interest),
    #             call_open_interest_notional=VALUES(call_open_interest_notional),
    #             put_open_interest_notional=VALUES(put_open_interest_notional),
    #             updated_at=CURRENT_TIMESTAMP
    #         """
    #
    #         try:
    #             with self.conn.cursor() as cur:
    #                 for row in rows:
    #                     cur.execute(sql, (
    #                         symbol, exchange, row.get("date"),
    #                         row.get("call_open_interest_market_value"),
    #                         row.get("put_open_interest"),
    #                         row.get("put_open_interest_market_value"),
    #                         row.get("max_pain_price"),
    #                         row.get("call_open_interest"),
    #                         row.get("call_open_interest_notional"),
    #                         row.get("put_open_interest_notional")
    #                     ))
    #             self.conn.commit()
    #             return len(rows)
    #         except Exception as e:
    #             self.conn.rollback()
    #             self.logger.error(f"Error upserting option_max_pain: {e}")
    #             return 0
    #
    #     def upsert_option_info(self, symbol: str, rows: List[Dict]) -> int:
    #         """Upsert option info data."""
    #         if not rows:
    #             return 0
    #
    #         sql = """
    #         INSERT INTO cg_option_info (
    #             symbol, exchange, open_interest, oi_market_share, open_interest_change_24h,
    #             open_interest_usd, volume_usd_24h, volume_change_percent_24h
    #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #         ON DUPLICATE KEY UPDATE
    #             open_interest=VALUES(open_interest),
    #             oi_market_share=VALUES(oi_market_share),
    #             open_interest_change_24h=VALUES(open_interest_change_24h),
    #             open_interest_usd=VALUES(open_interest_usd),
    #             volume_usd_24h=VALUES(volume_usd_24h),
    #             volume_change_percent_24h=VALUES(volume_change_percent_24h),
    #             updated_at=CURRENT_TIMESTAMP
    #         """
    #
    #         try:
    #             with self.conn.cursor() as cur:
    #                 for row in rows:
    #                     cur.execute(sql, (
    #                         symbol, row.get("exchange_name"),
    #                         row.get("open_interest"),
    #                         row.get("oi_market_share"),
    #                         row.get("open_interest_change_24h"),
    #                         row.get("open_interest_usd"),
    #                         row.get("volume_usd_24h"),
    #                         row.get("volume_change_percent_24h")
    #                     ))
    #             self.conn.commit()
    #             return len(rows)
    #         except Exception as e:
    #             self.conn.rollback()
    #             self.logger.error(f"Error upserting option_info: {e}")
    #             return 0

    # ===== ON-CHAIN DATA =====
    # def upsert_exchange_assets(self, exchange: str, rows: List[Dict]) -> int:
    #     """Upsert exchange assets data."""
    #     if not rows:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_exchange_assets (
    #         wallet_address, balance, balance_usd, symbol, assets_name, price, exchange
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         balance=VALUES(balance),
    #         balance_usd=VALUES(balance_usd),
    #         assets_name=VALUES(assets_name),
    #         price=VALUES(price),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     row.get("wallet_address"),
    #                     row.get("balance"),
    #                     row.get("balance_usd"),
    #                     row.get("symbol"),
    #                     row.get("assets_name"),
    #                     row.get("price"),
    #                     exchange
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting exchange_assets: {e}")
    #         return 0

    # def upsert_exchange_onchain_transfers(self, rows: List[Dict]) -> int:
    #     """Upsert exchange on-chain transfers data."""
    #     if not rows:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_exchange_onchain_transfers (
    #         transaction_hash, asset_symbol, amount_usd, asset_quantity, exchange_name,
    #         transfer_type, from_address, to_address, transaction_time
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         amount_usd=VALUES(amount_usd),
    #         asset_quantity=VALUES(asset_quantity),
    #         exchange_name=VALUES(exchange_name),
    #         transfer_type=VALUES(transfer_type),
    #         from_address=VALUES(from_address),
    #         to_address=VALUES(to_address),
    #         transaction_time=VALUES(transaction_time),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     row.get("transaction_hash"),
    #                     row.get("asset_symbol"),
    #                     row.get("amount_usd"),
    #                     row.get("asset_quantity"),
    #                     row.get("exchange_name"),
    #                     row.get("transfer_type"),
    #                     row.get("from_address"),
    #                     row.get("to_address"),
    #                     row.get("transaction_time")
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting exchange_onchain_transfers - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting exchange_onchain_transfers - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    # ===== SPOT ORDERBOOK =====
    # DISABLED - Commented out as per user request
    # def upsert_spot_orderbook_history(self, exchange: str, pair: str, interval: str, range_percent: str, rows: List[Dict]) -> int:
    #     """Upsert spot orderbook history data."""
    #     if not rows:
    #         return 0
    #
    #     sql = """
    #     INSERT INTO cg_spot_orderbook_history (
    #         exchange, pair, `interval`, range_percent, time, bids_usd, bids_quantity, asks_usd, asks_quantity
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         bids_usd=VALUES(bids_usd),
    #         bids_quantity=VALUES(bids_quantity),
    #         asks_usd=VALUES(asks_usd),
    #         asks_quantity=VALUES(asks_quantity),
    #         updated_at=CURRENT_TIMESTAMP
    #     """
    #
    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     exchange, pair, interval, range_percent,
    #                     row.get("time"),
    #                     row.get("bids_usd"),
    #                     row.get("bids_quantity"),
    #                     row.get("asks_usd"),
    #                     row.get("asks_quantity")
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting spot_orderbook_history: {e}")
    #         return 0
    #
    # def upsert_spot_orderbook_aggregated(self, exchange_list: str, symbol: str, interval: str, range_percent: str, rows: List[Dict]) -> int:
    #     """Upsert spot orderbook aggregated data."""
    #     if not rows:
    #         return 0
    #
    #     sql = """
    #     INSERT INTO cg_spot_orderbook_aggregated (
    #         exchange_list, symbol, `interval`, range_percent, time,
    #         aggregated_bids_usd, aggregated_bids_quantity, aggregated_asks_usd, aggregated_asks_quantity
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         aggregated_bids_usd=VALUES(aggregated_bids_usd),
    #         aggregated_bids_quantity=VALUES(aggregated_bids_quantity),
    #         aggregated_asks_usd=VALUES(aggregated_asks_usd),
    #         aggregated_asks_quantity=VALUES(aggregated_asks_quantity),
    #         updated_at=CURRENT_TIMESTAMP
    #     """
    #
    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 cur.execute(sql, (
    #                     exchange_list, symbol, interval, range_percent,
    #                     row.get("time"),
    #                     row.get("aggregated_bids_usd"),
    #                     row.get("aggregated_bids_quantity"),
    #                     row.get("aggregated_asks_usd"),
    #                     row.get("aggregated_asks_quantity")
    #                 ))
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting spot_orderbook_aggregated: {e}")
    #         return 0

    # ===== SPOT MARKET =====

    # def upsert_spot_coins_markets(self, rows: List[Dict]) -> int:
    #     """Upsert spot coins markets data."""
    #     if not rows:
    #         return 0

    #     # Build the SQL dynamically to avoid format string issues
    #     columns = [
    #         "symbol", "current_price", "market_cap",
    #         "price_change_5m", "price_change_15m", "price_change_30m", "price_change_1h",
    #         "price_change_4h", "price_change_12h", "price_change_24h", "price_change_1w",
    #         "price_change_percent_5m", "price_change_percent_15m", "price_change_percent_30m",
    #         "price_change_percent_1h", "price_change_percent_4h", "price_change_percent_12h",
    #         "price_change_percent_24h", "price_change_percent_1w",
    #         "volume_usd_1h", "volume_usd_5m", "volume_usd_15m", "volume_usd_30m",
    #         "volume_usd_4h", "volume_usd_12h", "volume_usd_24h", "volume_usd_1w",
    #         "volume_change_usd_1h", "volume_change_usd_5m", "volume_change_usd_15m",
    #         "volume_change_usd_30m", "volume_change_usd_4h", "volume_change_usd_12h",
    #         "volume_change_usd_24h", "volume_change_usd_1w",
    #         "volume_change_percent_1h", "volume_change_percent_5m", "volume_change_percent_15m",
    #         "volume_change_percent_30m", "volume_change_percent_4h", "volume_change_percent_12h",
    #         "volume_change_percent_24h", "volume_change_percent_1w",
    #         "buy_volume_usd_1h", "buy_volume_usd_5m", "buy_volume_usd_15m", "buy_volume_usd_30m",
    #         "buy_volume_usd_4h", "buy_volume_usd_12h", "buy_volume_usd_24h", "buy_volume_usd_1w",
    #         "sell_volume_usd_1h", "sell_volume_usd_5m", "sell_volume_usd_15m", "sell_volume_usd_30m",
    #         "sell_volume_usd_4h", "sell_volume_usd_12h", "sell_volume_usd_24h", "sell_volume_usd_1w",
    #         "volume_flow_usd_1h", "volume_flow_usd_5m", "volume_flow_usd_15m", "volume_flow_usd_30m",
    #         "volume_flow_usd_4h", "volume_flow_usd_12h", "volume_flow_usd_24h", "volume_flow_usd_1w"
    #     ]

    #     placeholders = ", ".join(["%s"] * len(columns))
    #     update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

    #     sql = f"""
    #     INSERT INTO cg_spot_coins_markets ({', '.join(columns)})
    #     VALUES ({placeholders})
    #     ON DUPLICATE KEY UPDATE
    #         {update_clauses},
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 values = [row.get(col) for col in columns]
    #                 cur.execute(sql, values)
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting spot_coins_markets: {e}")
    #         return 0

    # def upsert_spot_pairs_markets(self, rows: List[Dict]) -> int:
    #     """Upsert spot pairs markets data."""
    #     if not rows:
    #         return 0

    #     # Build the SQL dynamically to avoid format string issues
    #     columns = [
    #         "symbol", "exchange_name", "current_price",
    #         "price_change_1h", "price_change_percent_1h", "volume_usd_1h",
    #         "buy_volume_usd_1h", "sell_volume_usd_1h", "volume_change_usd_1h",
    #         "volume_change_percent_1h", "net_flows_usd_1h",
    #         "price_change_4h", "price_change_percent_4h", "volume_usd_4h",
    #         "buy_volume_usd_4h", "sell_volume_usd_4h", "volume_change_4h",
    #         "volume_change_percent_4h", "net_flows_usd_4h",
    #         "price_change_12h", "price_change_percent_12h", "volume_usd_12h",
    #         "buy_volume_usd_12h", "sell_volume_usd_12h", "volume_change_12h",
    #         "volume_change_percent_12h", "net_flows_usd_12h",
    #         "price_change_24h", "price_change_percent_24h", "volume_usd_24h",
    #         "buy_volume_usd_24h", "sell_volume_usd_24h", "volume_change_24h",
    #         "volume_change_percent_24h", "net_flows_usd_24h",
    #         "price_change_1w", "price_change_percent_1w", "volume_usd_1w",
    #         "buy_volume_usd_1w", "sell_volume_usd_1w", "volume_change_usd_1w",
    #         "volume_change_percent_1w", "net_flows_usd_1w"
    #     ]

    #     placeholders = ", ".join(["%s"] * len(columns))
    #     update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

    #     sql = f"""
    #     INSERT INTO cg_spot_pairs_markets ({', '.join(columns)})
    #     VALUES ({placeholders})
    #     ON DUPLICATE KEY UPDATE
    #         {update_clauses},
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 values = [row.get(col) for col in columns]
    #                 cur.execute(sql, values)
    #         self.conn.commit()
    #         return len(rows)
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(f"Error upserting spot_pairs_markets: {e}")
    #         return 0

    def upsert_spot_price_history(self, exchange: str, symbol: str, interval: str, rows: List[Dict]) -> int:
        """Upsert spot price history data."""
        if not rows:
            return 0

        sql = """
        INSERT INTO cg_spot_price_history (
            exchange, symbol, `interval`, time, open, high, low, close, volume_usd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open),
            high=VALUES(high),
            low=VALUES(low),
            close=VALUES(close),
            volume_usd=VALUES(volume_usd),
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, (
                        exchange, symbol, interval,
                        row.get("time"),
                        row.get("open"),
                        row.get("high"),
                        row.get("low"),
                        row.get("close"),
                        row.get("volume_usd")
                    ))
            self.conn.commit()
            return len(rows)
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error upserting spot_price_history: {e}")
            return 0

    # ===== BITCOIN ETF =====
    def upsert_bitcoin_etf_list(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert Bitcoin ETF list data with duplicate detection."""
        result = {
            "bitcoin_etf_list": 0,
            "bitcoin_etf_list_duplicates": 0,
            "bitcoin_etf_list_filtered": 0
        }

        if not rows:
            return result

        # Filter out rows where specified fields are null or have no value
        filtered_rows = []
        for row in rows:
            asset_details = row.get("asset_details", {})

            # Check the fields that should not be null or empty
            management_fee_percent = row.get("management_fee_percent")
            btc_change_percent_24h = asset_details.get("change_percent_24h")
            btc_change_24h = asset_details.get("change_quantity_24h")
            btc_change_percent_7d = asset_details.get("change_percent_7d")
            btc_change_7d = asset_details.get("change_quantity_7d")

            # Skip if any of these fields are null, None, empty string, or missing
            if (management_fee_percent is None or management_fee_percent == "" or
                btc_change_percent_24h is None or btc_change_percent_24h == "" or
                btc_change_24h is None or btc_change_24h == "" or
                btc_change_percent_7d is None or btc_change_percent_7d == "" or
                btc_change_7d is None or btc_change_7d == ""):

                self.logger.debug(f"Skipping ETF record with null/empty required fields: ticker={row.get('ticker')}")
                continue

            filtered_rows.append(row)

        if not filtered_rows:
            self.logger.info("No valid ETF records after filtering out null/empty values")
            return result

        # Log how many rows were filtered out
        filtered_count = len(rows) - len(filtered_rows)
        result["bitcoin_etf_list_filtered"] = filtered_count
        if filtered_count > 0:
            self.logger.info(f"Filtered out {filtered_count} ETF records with null/empty values from {len(rows)} total records")

        # Build the SQL dynamically to avoid format string issues
        columns = [
            "ticker", "fund_name", "region", "market_status", "primary_exchange",
            "cik_code", "fund_type", "market_cap_usd", "list_date", "shares_outstanding", "aum_usd",
            "management_fee_percent", "last_trade_time", "last_quote_time",
            "volume_quantity", "volume_usd", "price_usd", "price_change_usd", "price_change_percent",
            "btc_holding", "net_asset_value_usd", "premium_discount_percent",
            "btc_change_percent_24h", "btc_change_24h", "btc_change_percent_7d",
            "btc_change_7d", "update_date", "update_timestamp"
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

        sql = f"""
        INSERT INTO cg_bitcoin_etf_list ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {update_clauses},
            updated_at=CURRENT_TIMESTAMP
        """

        total_inserted = 0
        total_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in filtered_rows:
                    # Extract nested asset_details and map to correct field names
                    asset_details = row.get("asset_details", {})
                    values = [
                        row.get("ticker"),
                        row.get("fund_name"),
                        row.get("region"),
                        row.get("market_status"),
                        row.get("primary_exchange"),
                        row.get("cik_code"),
                        row.get("fund_type"),
                        row.get("market_cap_usd"),
                        row.get("list_date"),
                        row.get("shares_outstanding"),
                        row.get("aum_usd"),
                        row.get("management_fee_percent"),
                        row.get("last_trade_time"),
                        row.get("last_quote_time"),
                        row.get("volume_quantity"),
                        row.get("volume_usd"),
                        # These fields are at the top level, not in asset_details
                        row.get("price_usd"),  # price_usd is the current price
                        row.get("price_change_usd"),
                        row.get("price_change_percent"),
                        # Map asset_details fields to our schema
                        asset_details.get("holding_quantity"),  # btc_holding
                        asset_details.get("net_asset_value_usd"),
                        asset_details.get("premium_discount_percent"),
                        asset_details.get("change_percent_24h"),  # btc_change_percent_24h
                        asset_details.get("change_quantity_24h"),  # btc_change_24h
                        asset_details.get("change_percent_7d"),   # btc_change_percent_7d
                        asset_details.get("change_quantity_7d"),   # btc_change_7d
                        asset_details.get("update_date"),
                        row.get("update_timestamp")
                    ]
                    cur.execute(sql, values)

                    # Get affected rows count (1 = insert, 2 = update)
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1

            self.conn.commit()

            # Set the results
            result["bitcoin_etf_list"] = total_inserted  # Fresh records (inserted)
            result["bitcoin_etf_list_duplicates"] = total_updated  # Duplicate records (updated)

            if total_updated > 0:
                self.logger.info(f"Inserted {total_inserted} fresh ETF records, updated {total_updated} existing ETF records")

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting bitcoin_etf_list - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {"bitcoin_etf_list": 0, "bitcoin_etf_list_duplicates": 0, "bitcoin_etf_list_filtered": filtered_count}
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting bitcoin_etf_list - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {"bitcoin_etf_list": 0, "bitcoin_etf_list_duplicates": 0, "bitcoin_etf_list_filtered": filtered_count}

    def upsert_bitcoin_etf_premium_discount_history(self, rows: List[Dict], ticker: Optional[str] = None) -> Dict[str, int]:
        """Upsert Bitcoin ETF premium/discount history data with duplicate detection."""
        result = {
            "bitcoin_etf_premium_discount": 0,
            "bitcoin_etf_premium_discount_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_bitcoin_etf_premium_discount_history (
            timestamp, ticker, nav_usd, market_price_usd, premium_discount_details
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nav_usd=VALUES(nav_usd), market_price_usd=VALUES(market_price_usd),
            premium_discount_details=VALUES(premium_discount_details),
            updated_at=CURRENT_TIMESTAMP
        """

        total_inserted = 0
        total_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    # Get values from row - ticker is now in the row data
                    timestamp = row.get("timestamp")
                    row_ticker = row.get("ticker")  # Ticker from the flattened data
                    nav_usd = row.get("nav_usd")
                    market_price_usd = row.get("market_price_usd")
                    premium_discount_details = row.get("premium_discount_details")

                    # Use ticker from row if available, otherwise fall back to parameter
                    final_ticker = row_ticker if row_ticker else ticker

                    # Skip if timestamp is None (required field)
                    if timestamp is None:
                        self.logger.warning(
                            f"Skipping ETF premium/discount record with null timestamp: {row}"
                        )
                        continue

                    # Log warning if critical fields are null and skip
                    if (
                        nav_usd is None
                        and market_price_usd is None
                        and premium_discount_details is None
                    ):
                        self.logger.warning(
                            f"ETF premium/discount record has all null values for ticker={final_ticker}, "
                            f"timestamp={timestamp}. Skipping. Raw data: {row}"
                        )
                        continue

                    cur.execute(
                        sql,
                        (
                            timestamp,
                            final_ticker,  # Use ticker from row or parameter
                            nav_usd,
                            market_price_usd,
                            premium_discount_details,
                        ),
                    )

                    # Get affected rows count (1 = insert, 2 = update)
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1

            self.conn.commit()

            # Set the results
            result["bitcoin_etf_premium_discount"] = total_inserted  # Fresh records (inserted)
            result["bitcoin_etf_premium_discount_duplicates"] = total_updated  # Duplicate records (updated)

            ticker_label = ticker if ticker else "aggregated"
            if total_updated > 0:
                self.logger.info(f"Inserted {total_inserted} fresh premium/discount records, updated {total_updated} existing records for {ticker_label}")

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            ticker_label = ticker if ticker else "aggregated"
            self.logger.error(
                f"Database error upserting bitcoin_etf_premium_discount_history for {ticker_label} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {"bitcoin_etf_premium_discount": 0, "bitcoin_etf_premium_discount_duplicates": 0}
        except Exception as e:
            self.conn.rollback()
            ticker_label = ticker if ticker else "aggregated"
            self.logger.error(
                f"Unexpected error upserting bitcoin_etf_premium_discount_history for {ticker_label} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {"bitcoin_etf_premium_discount": 0, "bitcoin_etf_premium_discount_duplicates": 0}

    def upsert_bitcoin_etf_flows_history(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert Bitcoin ETF flows history data with duplicate detection."""
        result = {
            "bitcoin_etf_flows": 0,
            "bitcoin_etf_flows_duplicates": 0,
            "bitcoin_etf_flows_details": 0,
            "bitcoin_etf_flows_details_duplicates": 0
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_bitcoin_etf_flows_history (
            timestamp, flow_usd
        ) VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            flow_usd=VALUES(flow_usd),
            updated_at=CURRENT_TIMESTAMP
        """

        etf_flows_sql = """
        INSERT INTO cg_bitcoin_etf_flows_details (
            timestamp, etf_ticker, flow_usd
        ) VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            flow_usd=VALUES(flow_usd),
            updated_at=CURRENT_TIMESTAMP
        """

        total_flows_inserted = 0
        total_flows_updated = 0
        total_details_inserted = 0
        total_details_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    # Skip if flow_usd is None or 0.00000000
                    flow_usd = row.get("flow_usd")
                    if flow_usd is not None and flow_usd != 0:
                        # Insert main flows record
                        cur.execute(sql, (row.get("timestamp"), flow_usd))

                        # Get affected rows count for main flows (1 = insert, 2 = update)
                        affected_rows = cur.rowcount
                        if affected_rows == 1:
                            total_flows_inserted += 1
                        elif affected_rows == 2:
                            total_flows_updated += 1

                    # Insert individual ETF flows
                    etf_flows = row.get("etf_flows", [])
                    for etf_flow in etf_flows:
                        etf_flow_usd = etf_flow.get("flow_usd")
                        # Skip if flow_usd is None or 0.00000000
                        if etf_flow_usd is not None and etf_flow_usd != 0:
                            cur.execute(
                                etf_flows_sql,
                                (
                                    row.get("timestamp"),
                                    etf_flow.get("etf_ticker"),
                                    etf_flow_usd,
                                ),
                            )

                            # Get affected rows count for details (1 = insert, 2 = update)
                            affected_rows = cur.rowcount
                            if affected_rows == 1:
                                total_details_inserted += 1
                            elif affected_rows == 2:
                                total_details_updated += 1

            self.conn.commit()

            # Set the results
            result["bitcoin_etf_flows"] = total_flows_inserted
            result["bitcoin_etf_flows_duplicates"] = total_flows_updated
            result["bitcoin_etf_flows_details"] = total_details_inserted
            result["bitcoin_etf_flows_details_duplicates"] = total_details_updated

            if total_flows_updated > 0 or total_details_updated > 0:
                self.logger.info(
                    f"ETF Flows: Inserted {total_flows_inserted} fresh records, updated {total_flows_updated} existing records. "
                    f"Details: Inserted {total_details_inserted} fresh records, updated {total_details_updated} existing records"
                )

            return result

        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting bitcoin_etf_flows_history - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return {
                "bitcoin_etf_flows": 0,
                "bitcoin_etf_flows_duplicates": 0,
                "bitcoin_etf_flows_details": 0,
                "bitcoin_etf_flows_details_duplicates": 0
            }
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting bitcoin_etf_flows_history - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return {
                "bitcoin_etf_flows": 0,
                "bitcoin_etf_flows_duplicates": 0,
                "bitcoin_etf_flows_details": 0,
                "bitcoin_etf_flows_details_duplicates": 0
            }

    # ===== TRADING MARKET =====
    # def upsert_supported_exchange_pairs(self, data: Dict) -> int:
    #     """Upsert supported exchange pairs data."""
    #     if not data:
    #         return 0

    #     sql = """
    #     INSERT INTO cg_supported_exchange_pairs (
    #         exchange_name, instrument_id, base_asset, quote_asset
    #     ) VALUES (%s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         base_asset=VALUES(base_asset),
    #         quote_asset=VALUES(quote_asset),
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         saved = 0
    #         with self.conn.cursor() as cur:
    #             for exchange_name, pairs in data.items():
    #                 for pair in pairs:
    #                     cur.execute(sql, (
    #                         exchange_name,
    #                         pair.get("instrument_id"),
    #                         pair.get("base_asset"),
    #                         pair.get("quote_asset")
    #                     ))
    #                     saved += 1
    #         self.conn.commit()
    #         return saved
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting supported_exchange_pairs - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting supported_exchange_pairs - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    # def upsert_pairs_markets(self, rows: List[Dict]) -> int:
    #     """Upsert pairs markets data."""
    #     if not rows:
    #         return 0

    #     # Build SQL dynamically to handle all fields
    #     columns = [
    #         "instrument_id", "exchange_name", "symbol", "current_price", "index_price",
    #         "price_change_percent_24h", "volume_usd", "volume_usd_change_percent_24h",
    #         "long_volume_usd", "short_volume_usd", "long_volume_quantity", "short_volume_quantity",
    #         "open_interest_quantity", "open_interest_usd", "open_interest_change_percent_24h",
    #         "long_liquidation_usd_24h", "short_liquidation_usd_24h", "funding_rate",
    #         "next_funding_time", "open_interest_volume_radio", "oi_vol_ratio_change_percent_24h"
    #     ]

    #     placeholders = ", ".join(["%s"] * len(columns))
    #     update_clauses = ", ".join([f"{col}=VALUES({col})" for col in columns])

    #     sql = f"""
    #     INSERT INTO cg_pairs_markets ({', '.join(columns)})
    #     VALUES ({placeholders})
    #     ON DUPLICATE KEY UPDATE
    #         {update_clauses},
    #         updated_at=CURRENT_TIMESTAMP
    #     """

    #     try:
    #         with self.conn.cursor() as cur:
    #             for row in rows:
    #                 values = [row.get(col) for col in columns]
    #                 cur.execute(sql, values)
    #         self.conn.commit()
    #         return len(rows)
    #     except pymysql.Error as e:
    #         self.conn.rollback()
    #         error_code = e.args[0] if e.args else 'unknown'
    #         error_msg = e.args[1] if len(e.args) > 1 else str(e)
    #         self.logger.error(
    #             f"Database error upserting pairs_markets - "
    #             f"Error code: {error_code}, Message: {error_msg}"
    #         )
    #         return 0
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.logger.error(
    #             f"Unexpected error upserting pairs_markets - "
    #             f"Type: {type(e).__name__}, Message: {str(e)}"
    #         )
    #         return 0

    # ========== Macro Overlay ==========
    def upsert_bitcoin_vs_global_m2_growth(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert Bitcoin vs Global M2 Growth data with duplicate detection."""
        result = {
            "bitcoin_vs_global_m2_growth": 0,
            "bitcoin_vs_global_m2_growth_duplicates": 0,
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_bitcoin_vs_global_m2_growth (
            timestamp, price, global_m2_yoy_growth, global_m2_supply
        )
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            global_m2_yoy_growth = VALUES(global_m2_yoy_growth),
            global_m2_supply = VALUES(global_m2_supply)
        """

        total_inserted = 0
        total_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        sql,
                        (
                            row.get("timestamp"),
                            row.get("price"),
                            row.get("global_m2_yoy_growth"),
                            row.get("global_m2_supply"),
                        ),
                    )
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1
            self.conn.commit()
            result["bitcoin_vs_global_m2_growth"] = total_inserted
            result["bitcoin_vs_global_m2_growth_duplicates"] = total_updated
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            self.logger.error(f"Database error upserting bitcoin_vs_global_m2_growth: {e}")
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error upserting bitcoin_vs_global_m2_growth: {e}")
            return result

    # ========== Options ==========
    def upsert_option_exchange_oi_history(
        self, symbol: str, unit: str, range_param: str, data: Dict
    ) -> Dict[str, int]:
        """Upsert Option Exchange OI History data with relational structure and duplicate detection."""
        result = {
            "option_exchange_oi_history": 0,
            "option_exchange_oi_history_duplicates": 0,
        }

        try:
            with self.conn.cursor() as cur:
                # Step 1: Insert or update main record
                main_sql = """
                INSERT INTO cg_option_exchange_oi_history (
                    symbol, unit, `range`
                )
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    updated_at = CURRENT_TIMESTAMP
                """

                cur.execute(main_sql, (symbol, unit, range_param))

                # Get the main record ID
                if cur.rowcount == 1:
                    # New insert
                    main_affected = 1
                    option_exchange_oi_history_id = cur.lastrowid
                else:
                    # Update - get existing ID
                    main_affected = 2
                    get_id_sql = "SELECT id FROM cg_option_exchange_oi_history WHERE symbol = %s AND unit = %s AND `range` = %s"
                    cur.execute(get_id_sql, (symbol, unit, range_param))
                    result_row = cur.fetchone()
                    option_exchange_oi_history_id = result_row[0] if result_row else None

                if not option_exchange_oi_history_id:
                    raise Exception("Failed to get option_exchange_oi_history_id")

                # Step 2: Delete existing child records for updates
                if main_affected == 2:
                    delete_time_sql = "DELETE FROM cg_option_exchange_oi_history_time_list WHERE option_exchange_oi_history_id = %s"
                    delete_exchange_sql = "DELETE FROM cg_option_exchange_oi_history_exchange_data WHERE option_exchange_oi_history_id = %s"
                    cur.execute(delete_time_sql, (option_exchange_oi_history_id,))
                    cur.execute(delete_exchange_sql, (option_exchange_oi_history_id,))

                # Step 3: Extract data from API response structure
                # The API response structure: data.data_map contains arrays of OI values
                time_list = []
                if isinstance(data, dict) and "data" in data and len(data["data"]) > 0:
                    main_data = data["data"][0]  # Get first data object

                    # Extract time_list and data_map from the correct location
                    time_list = main_data.get("time_list", [])
                    data_map = main_data.get("data_map", {})

                    # Step 4: Insert time_list data
                    if isinstance(time_list, list) and time_list:
                        time_values = [(option_exchange_oi_history_id, time_val) for time_val in time_list]
                        time_sql = """
                        INSERT INTO cg_option_exchange_oi_history_time_list (
                            option_exchange_oi_history_id, time_value
                        )
                        VALUES (%s, %s)
                        """
                        cur.executemany(time_sql, time_values)

                    # Step 5: Insert exchange_data from data_map
                    if isinstance(data_map, dict):
                        for exchange_name, oi_values in data_map.items():
                            if isinstance(oi_values, list):
                                # Insert each OI value with its corresponding timestamp
                                for i, oi_value in enumerate(oi_values):
                                    if i < len(time_list):  # Ensure we have corresponding timestamp
                                        try:
                                            # Convert to decimal
                                            oi_decimal = float(oi_value) if oi_value else 0.0
                                            timestamp = time_list[i]

                                            exchange_sql = """
                                            INSERT INTO cg_option_exchange_oi_history_exchange_data (
                                                option_exchange_oi_history_id, exchange_name, oi_value, timestamp_value
                                            )
                                            VALUES (%s, %s, %s, %s)
                                            """
                                            cur.execute(exchange_sql, (option_exchange_oi_history_id, exchange_name, oi_decimal, timestamp))
                                        except (ValueError, TypeError):
                                            # Skip invalid numeric values
                                            continue

                # Update result counts
                if main_affected == 1:
                    result["option_exchange_oi_history"] = 1
                else:
                    result["option_exchange_oi_history_duplicates"] = 1

            self.conn.commit()
            return result

        except pymysql.Error as e:
            self.conn.rollback()
            self.logger.error(f"Database error upserting option_exchange_oi_history: {e}")
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error upserting option_exchange_oi_history: {e}")
            return result

    def get_option_exchange_oi_history_with_relations(
        self, symbol: str = None, unit: str = None, range_param: str = None, limit: int = 100
    ) -> list:
        """
        Retrieve Option Exchange OI History data with time_list and exchange_data relations.

        Args:
            symbol: Filter by symbol (optional)
            unit: Filter by unit (optional)
            range_param: Filter by range (optional)
            limit: Maximum number of main records to return

        Returns:
            List of dictionaries with complete relational data
        """
        try:
            with self.conn.cursor() as cur:
                # Build WHERE clause
                conditions = []
                params = []

                if symbol:
                    conditions.append("h.symbol = %s")
                    params.append(symbol)
                if unit:
                    conditions.append("h.unit = %s")
                    params.append(unit)
                if range_param:
                    conditions.append("h.`range` = %s")
                    params.append(range_param)

                where_clause = " AND ".join(conditions) if conditions else "1=1"
                params.append(limit)

                # Get main records first
                main_query = f"""
                SELECT
                    h.id, h.symbol, h.unit, h.`range`, h.created_at, h.updated_at
                FROM cg_option_exchange_oi_history h
                WHERE {where_clause}
                ORDER BY h.updated_at DESC
                LIMIT %s
                """

                cur.execute(main_query, tuple(params))
                main_rows = cur.fetchall()

                results = []
                for main_row in main_rows:
                    option_exchange_oi_history_id = main_row[0]

                    # Get time_list for this record
                    time_query = """
                    SELECT time_value
                    FROM cg_option_exchange_oi_history_time_list
                    WHERE option_exchange_oi_history_id = %s
                    ORDER BY time_value
                    """
                    cur.execute(time_query, (option_exchange_oi_history_id,))
                    time_rows = cur.fetchall()
                    time_list = [row[0] for row in time_rows]

                    # Get exchange_data for this record organized by exchange and timestamp
                    exchange_query = """
                    SELECT exchange_name, timestamp_value, oi_value
                    FROM cg_option_exchange_oi_history_exchange_data
                    WHERE option_exchange_oi_history_id = %s
                    ORDER BY exchange_name, timestamp_value
                    """
                    cur.execute(exchange_query, (option_exchange_oi_history_id,))
                    exchange_rows = cur.fetchall()

                    # Build data_map structure
                    data_map = {}
                    for exchange_row in exchange_rows:
                        exchange_name = exchange_row[0]
                        timestamp_value = exchange_row[1]
                        oi_value = float(exchange_row[2])

                        if exchange_name not in data_map:
                            data_map[exchange_name] = []

                        # Find the index of this timestamp in time_list and place oi_value accordingly
                        if timestamp_value in time_list:
                            index = time_list.index(timestamp_value)
                            # Ensure array is long enough
                            while len(data_map[exchange_name]) <= index:
                                data_map[exchange_name].append(None)
                            data_map[exchange_name][index] = oi_value

                    result_dict = {
                        'id': main_row[0],
                        'symbol': main_row[1],
                        'unit': main_row[2],
                        'range': main_row[3],
                        'created_at': main_row[4],
                        'updated_at': main_row[5],
                        'time_list': time_list,
                        'data_map': data_map
                    }
                    results.append(result_dict)

                return results

        except Exception as e:
            self.logger.error(f"Error retrieving option_exchange_oi_history with relations: {e}")
            return []

    # ========== Sentiment ==========
    def upsert_fear_greed_index(self, data: Dict) -> Dict[str, int]:
        """
        Upsert Fear & Greed Index data with parent-child relationship.

        Main table: cg_fear_greed_index
        Child table: cg_fear_greed_index_data_list (linked by fear_greed_index_id)
        """
        result = {
            "fear_greed_index": 0,
            "fear_greed_index_duplicates": 0,
            "fear_greed_index_data_list": 0,
            "fear_greed_index_data_list_duplicates": 0
        }

        data_list = data.get("data_list", [])
        if not data_list:
            self.logger.warning("No data_list found in fear_greed_index response")
            return result

        import time
        fetch_timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds

        main_sql = """
        INSERT INTO cg_fear_greed_index (fetch_timestamp)
        VALUES (%s)
        """

        child_sql = """
        INSERT INTO cg_fear_greed_index_data_list (
            fear_greed_index_id, index_value, sequence_order
        )
        VALUES (%s, %s, %s)
        """

        try:
            with self.conn.cursor() as cur:
                # Insert main record
                cur.execute(main_sql, (fetch_timestamp,))
                parent_id = cur.lastrowid
                result["fear_greed_index"] = 1

                # Insert child records
                total_inserted = 0
                for order, index_value in enumerate(data_list):
                    if index_value is not None:
                        cur.execute(child_sql, (parent_id, index_value, order))
                        if cur.rowcount == 1:
                            total_inserted += 1

                result["fear_greed_index_data_list"] = total_inserted

                self.logger.info(
                    f"Inserted Fear & Greed Index: parent_id={parent_id}, "
                    f"fetch_timestamp={fetch_timestamp}, data_list_count={total_inserted}"
                )

            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting fear_greed_index - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting fear_greed_index - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    def upsert_hyperliquid_whale_alert(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert Hyperliquid Whale Alert data with duplicate detection."""
        result = {
            "hyperliquid_whale_alert": 0,
            "hyperliquid_whale_alert_duplicates": 0,
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_hyperliquid_whale_alert (
            user, symbol, position_size, entry_price, liq_price,
            position_value_usd, position_action, create_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            position_size = VALUES(position_size),
            entry_price = VALUES(entry_price),
            liq_price = VALUES(liq_price),
            position_value_usd = VALUES(position_value_usd),
            position_action = VALUES(position_action)
        """

        total_inserted = 0
        total_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        sql,
                        (
                            row.get("user"),
                            row.get("symbol"),
                            row.get("position_size"),
                            row.get("entry_price"),
                            row.get("liq_price"),
                            row.get("position_value_usd"),
                            row.get("position_action"),
                            row.get("create_time"),
                        ),
                    )
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1
            self.conn.commit()
            result["hyperliquid_whale_alert"] = total_inserted
            result["hyperliquid_whale_alert_duplicates"] = total_updated
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            self.logger.error(f"Database error upserting hyperliquid_whale_alert: {e}")
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error upserting hyperliquid_whale_alert: {e}")
            return result

    def upsert_whale_transfer(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert Whale Transfer data with duplicate detection."""
        result = {
            "whale_transfer": 0,
            "whale_transfer_duplicates": 0,
        }

        if not rows:
            return result

        sql = """
        INSERT INTO cg_whale_transfer (
            transaction_hash, amount_usd, asset_quantity, asset_symbol,
            from_address, to_address, blockchain_name, block_height, block_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            amount_usd = VALUES(amount_usd),
            asset_quantity = VALUES(asset_quantity),
            asset_symbol = VALUES(asset_symbol),
            from_address = VALUES(from_address),
            to_address = VALUES(to_address),
            blockchain_name = VALUES(blockchain_name),
            block_height = VALUES(block_height),
            block_timestamp = VALUES(block_timestamp)
        """

        total_inserted = 0
        total_updated = 0

        try:
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        sql,
                        (
                            row.get("transaction_hash"),
                            row.get("amount_usd"),
                            row.get("asset_quantity"),
                            row.get("asset_symbol"),
                            row.get("from"),
                            row.get("to"),
                            row.get("blockchain_name"),
                            row.get("block_height"),
                            row.get("block_timestamp"),
                        ),
                    )
                    affected_rows = cur.rowcount
                    if affected_rows == 1:
                        total_inserted += 1
                    elif affected_rows == 2:
                        total_updated += 1
            self.conn.commit()
            result["whale_transfer"] = total_inserted
            result["whale_transfer_duplicates"] = total_updated
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            self.logger.error(f"Database error upserting whale_transfer: {e}")
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error upserting whale_transfer: {e}")
            return result
    # ===== SPOT ORDERBOOK =====
    def upsert_spot_orderbook_history(self, exchange: str, pair: str, interval: str, range_percent: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert spot orderbook history data."""
        result = {
            "spot_orderbook_history": 0,
            "spot_orderbook_history_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["spot_orderbook_history"] += 1
                    else:
                        result["spot_orderbook_history_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_orderbook_history for {exchange}:{pair}:{interval}:{range_percent} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_orderbook_history for {exchange}:{pair}:{interval}:{range_percent} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    def upsert_spot_orderbook_aggregated(self, exchange_list: str, symbol: str, interval: str, range_percent: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert spot orderbook aggregated data."""
        result = {
            "spot_orderbook_aggregated": 0,
            "spot_orderbook_aggregated_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["spot_orderbook_aggregated"] += 1
                    else:
                        result["spot_orderbook_aggregated_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_orderbook_aggregated for {exchange_list}:{symbol}:{interval}:{range_percent} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_orderbook_aggregated for {exchange_list}:{symbol}:{interval}:{range_percent} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== SPOT COINS MARKETS =====
    def upsert_spot_coins_markets(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert spot coins markets data."""
        result = {
            "spot_coins_markets": 0,
            "spot_coins_markets_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["spot_coins_markets"] += 1
                    else:
                        result["spot_coins_markets_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_coins_markets - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_coins_markets - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== SPOT PAIRS MARKETS =====
    def upsert_spot_pairs_markets(self, rows: List[Dict]) -> Dict[str, int]:
        """Upsert spot pairs markets data."""
        result = {
            "spot_pairs_markets": 0,
            "spot_pairs_markets_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["spot_pairs_markets"] += 1
                    else:
                        result["spot_pairs_markets_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_pairs_markets - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_pairs_markets - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== SPOT PRICE HISTORY =====
    def upsert_spot_price_history(self, exchange: str, symbol: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert spot price history data."""
        result = {
            "spot_price_history": 0,
            "spot_price_history_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["spot_price_history"] += 1
                    else:
                        result["spot_price_history_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting spot_price_history for {exchange}:{symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting spot_price_history for {exchange}:{symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    # ===== OPEN INTEREST AGGREGATED STABLECOIN HISTORY =====
    def upsert_open_interest_aggregated_stablecoin_history(self, exchange_list: str, symbol: str, interval: str, rows: List[Dict]) -> Dict[str, int]:
        """Upsert open interest aggregated stablecoin history (OHLC) data."""
        result = {
            "open_interest_aggregated_stablecoin_history": 0,
            "open_interest_aggregated_stablecoin_history_duplicates": 0
        }

        if not rows:
            return result

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
                    # Get affected rows count
                    affected = cur.rowcount
                    # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                    if affected == 1:
                        result["open_interest_aggregated_stablecoin_history"] += 1
                    else:
                        result["open_interest_aggregated_stablecoin_history_duplicates"] += 1
            self.conn.commit()
            return result
        except pymysql.Error as e:
            self.conn.rollback()
            error_code = e.args[0] if e.args else 'unknown'
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            self.logger.error(
                f"Database error upserting open_interest_aggregated_stablecoin_history for {exchange_list}:{symbol}:{interval} - "
                f"Error code: {error_code}, Message: {error_msg}"
            )
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(
                f"Unexpected error upserting open_interest_aggregated_stablecoin_history for {exchange_list}:{symbol}:{interval} - "
                f"Type: {type(e).__name__}, Message: {str(e)}"
            )
            return result

    
    # ===== NEW ENDPOINTS REPOSITORY METHODS =====

    def insert_futures_footprint_history(self, exchange: str, symbol: str, interval: str, data: List[List]) -> Dict[str, int]:
        """Insert futures footprint history data with duplicate checking."""
        result = {
            "futures_footprint_history": 0,
            "futures_footprint_history_duplicates": 0
        }

        if not data:
            return result

        sql = """
        INSERT INTO cg_futures_footprint_history (
            exchange, symbol, `interval`, time, price_start, price_end,
            taker_buy_volume, taker_sell_volume, taker_buy_volume_usd,
            taker_sell_volume_usd, taker_buy_trades, taker_sell_trades
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            taker_buy_volume=VALUES(taker_buy_volume),
            taker_sell_volume=VALUES(taker_sell_volume),
            taker_buy_volume_usd=VALUES(taker_buy_volume_usd),
            taker_sell_volume_usd=VALUES(taker_sell_volume_usd),
            taker_buy_trades=VALUES(taker_buy_trades),
            taker_sell_trades=VALUES(taker_sell_trades)
        """

        try:
            with self.conn.cursor() as cur:
                for timestamp, price_ranges in data:
                    for price_range in price_ranges:
                        if len(price_range) >= 8:
                            cur.execute(sql, (
                                exchange, symbol, interval, timestamp,
                                price_range[0],  # price_start
                                price_range[1],  # price_end
                                price_range[2],  # taker_buy_volume
                                price_range[3],  # taker_sell_volume
                                price_range[4],  # taker_buy_volume_usd
                                price_range[5],  # taker_sell_volume_usd
                                price_range[7],  # taker_buy_trades (index 6 is duplicate)
                                price_range[8] if len(price_range) > 8 else 0  # taker_sell_trades
                            ))
                            # Get affected rows count
                            affected = cur.rowcount
                            # rowcount = 1 for INSERT, 2 for UPDATE (but may vary)
                            if affected == 1:
                                result["futures_footprint_history"] += 1
                            else:
                                result["futures_footprint_history_duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting futures footprint history: {e}")
            return result

    def insert_spot_large_orderbook_history(self, exchange: str, symbol: str, state: str, data: List[Dict]) -> Dict[str, int]:
        """Insert spot large orderbook history data."""
        result = {
            "saved": 0,
            "duplicates": 0
        }

        if not data:
            return result

        sql = """
        INSERT INTO cg_spot_large_orderbook_history (
            id, exchange_name, symbol, base_asset, quote_asset, price,
            start_time, start_quantity, start_usd_value, current_quantity,
            current_usd_value, `current_time`, executed_volume, executed_usd_value,
            trade_count, order_side, order_state, order_end_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_quantity=VALUES(current_quantity),
            current_usd_value=VALUES(current_usd_value),
            `current_time`=VALUES(`current_time`),
            executed_volume=VALUES(executed_volume),
            executed_usd_value=VALUES(executed_usd_value),
            trade_count=VALUES(trade_count),
            order_state=VALUES(order_state),
            order_end_time=VALUES(order_end_time)
        """

        try:
            with self.conn.cursor() as cur:
                for row in data:
                    cur.execute(sql, (
                        row.get("id"), row.get("exchange_name"), row.get("symbol"),
                        row.get("base_asset"), row.get("quote_asset"), row.get("price"),
                        row.get("start_time"), row.get("start_quantity"), row.get("start_usd_value"),
                        row.get("current_quantity"), row.get("current_usd_value"), row.get("current_time"),
                        row.get("executed_volume"), row.get("executed_usd_value"), row.get("trade_count"),
                        row.get("order_side"), row.get("order_state"), row.get("order_end_time")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["saved"] += 1
                    elif cur.rowcount == 2:
                        result["duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting spot large orderbook history: {e}")
            return result

    def insert_spot_large_orderbook(self, exchange: str, symbol: str, data: List[Dict]) -> Dict[str, int]:
        """Insert current spot large orderbook data."""
        result = {
            "saved": 0,
            "duplicates": 0
        }

        if not data:
            return result

        sql = """
        INSERT INTO cg_spot_large_orderbook (
            id, exchange_name, symbol, base_asset, quote_asset, price,
            start_time, start_quantity, start_usd_value, current_quantity,
            current_usd_value, `current_time`, executed_volume, executed_usd_value,
            trade_count, order_side, order_state
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_quantity=VALUES(current_quantity),
            current_usd_value=VALUES(current_usd_value),
            `current_time`=VALUES(`current_time`),
            executed_volume=VALUES(executed_volume),
            executed_usd_value=VALUES(executed_usd_value),
            trade_count=VALUES(trade_count),
            order_state=VALUES(order_state)
        """

        try:
            with self.conn.cursor() as cur:
                for row in data:
                    cur.execute(sql, (
                        row.get("id"), row.get("exchange_name"), row.get("symbol"),
                        row.get("base_asset"), row.get("quote_asset"), row.get("price"),
                        row.get("start_time"), row.get("start_quantity"), row.get("start_usd_value"),
                        row.get("current_quantity"), row.get("current_usd_value"), row.get("current_time"),
                        row.get("executed_volume"), row.get("executed_usd_value"), row.get("trade_count"),
                        row.get("order_side"), row.get("order_state")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["saved"] += 1
                    elif cur.rowcount == 2:
                        result["duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting spot large orderbook: {e}")
            return result

    def insert_spot_aggregated_taker_volume_history(self, exchange_list: str, symbol: str, interval: str, unit: str, data: List[Dict]) -> Dict[str, int]:
        """Insert spot aggregated taker volume history data."""
        result = {
            "saved": 0,
            "duplicates": 0
        }

        if not data:
            return result

        sql = """
        INSERT INTO cg_spot_aggregated_taker_volume_history (
            exchange_list, symbol, `interval`, unit, time,
            aggregated_buy_volume_usd, aggregated_sell_volume_usd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            aggregated_buy_volume_usd=VALUES(aggregated_buy_volume_usd),
            aggregated_sell_volume_usd=VALUES(aggregated_sell_volume_usd)
        """

        try:
            with self.conn.cursor() as cur:
                for row in data:
                    cur.execute(sql, (
                        exchange_list, symbol, interval, unit,
                        row.get("time"),
                        row.get("aggregated_buy_volume_usd"),
                        row.get("aggregated_sell_volume_usd")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["saved"] += 1
                    elif cur.rowcount == 2:
                        result["duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting spot aggregated taker volume history: {e}")
            return result

    def insert_spot_taker_volume_history(self, exchange: str, symbol: str, interval: str, unit: str, data: List[Dict]) -> Dict[str, int]:
        """Insert spot taker volume history data."""
        result = {
            "saved": 0,
            "duplicates": 0
        }

        if not data:
            return result

        sql = """
        INSERT INTO cg_spot_taker_volume_history (
            exchange, symbol, `interval`, unit, time,
            aggregated_buy_volume_usd, aggregated_sell_volume_usd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            aggregated_buy_volume_usd=VALUES(aggregated_buy_volume_usd),
            aggregated_sell_volume_usd=VALUES(aggregated_sell_volume_usd)
        """

        try:
            with self.conn.cursor() as cur:
                for row in data:
                    cur.execute(sql, (
                        exchange, symbol, interval, unit,
                        row.get("time"),
                        row.get("aggregated_buy_volume_usd"),
                        row.get("aggregated_sell_volume_usd")
                    ))
                    # Get affected rows count (1 = insert, 2 = update)
                    if cur.rowcount == 1:
                        result["saved"] += 1
                    elif cur.rowcount == 2:
                        result["duplicates"] += 1
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting spot taker volume history: {e}")
            return result
