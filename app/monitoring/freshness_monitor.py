# app/monitoring/freshness_monitor.py
import logging
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

# Define WIB timezone (UTC+7)
class WIB(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=7)
    def tzname(self, dt):
        return "WIB"
    def dst(self, dt):
        return timedelta(0)

wib_tz = WIB()

logger = logging.getLogger(__name__)


class FreshnessStatus(Enum):
    VERY_FRESH = "very_fresh"      # < 1 hour old
    FRESH = "fresh"                # < 6 hours old
    MODERATE = "moderate"          # < 24 hours old
    STALE = "stale"                # >= 24 hours old
    NO_DATA = "no_data"           # No records found
    ERROR = "error"               # Error checking freshness


@dataclass
class FreshnessConfig:
    """Configuration for freshness monitoring per data stream."""
    table_name: str
    time_column: str
    time_format: str = "timestamp_ms"  # "timestamp_ms", "timestamp_s", "datetime"
    expected_max_age_hours: Dict[FreshnessStatus, float] = None

    def __post_init__(self):
        if self.expected_max_age_hours is None:
            self.expected_max_age_hours = {
                FreshnessStatus.VERY_FRESH: 1.0,
                FreshnessStatus.FRESH: 6.0,
                FreshnessStatus.MODERATE: 24.0,
            }


@dataclass
class FreshnessResult:
    """Result of freshness check for a data stream."""
    stream_name: str
    status: FreshnessStatus
    total_records: int
    latest_timestamp: Optional[int]
    latest_datetime: Optional[str]
    earliest_timestamp: Optional[int]
    hours_since_latest: Optional[float]
    records_last_24h: int  # Now represents records in current cycle
    avg_records_per_hour: Optional[float]
    data_gaps_hours: List[int]
    cycle_duration_minutes: Optional[float] = None  # Time taken for last cycle
    error_message: Optional[str] = None
    recommendations: List[str] = None

    def __post_init__(self):
        if self.recommendations is None:
            self.recommendations = []


class DataFreshnessMonitor:
    """Monitor data freshness across all Coinglass pipelines."""

    def __init__(self, connection):
        self.conn = connection
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Configuration for all data streams
        self.stream_configs = {
            # Futures Data
            "funding_rate": FreshnessConfig("cg_funding_rate_history", "time"),
            # "oi_history": FreshnessConfig("cg_open_interest_history", "time"),  # DISABLED
            # "oi_exchange_list": FreshnessConfig("cg_open_interest_exchange_list", "updated_at", "datetime"),  # DISABLED - Table deleted
            "oi_aggregated_history": FreshnessConfig(
                "cg_open_interest_aggregated_history", "time"
            ),  # ACTIVE in oi_aggregated_history pipeline
            "long_short_ratio_top_account": FreshnessConfig(
                "cg_long_short_top_account_ratio_history", "time"
            ),
            "long_short_ratio_global_account": FreshnessConfig(
                "cg_long_short_global_account_ratio_history", "time"
            ),
            # "long_short_ratio_position": FreshnessConfig("cg_long_short_position_ratio_history", "time"),  # DISABLED
            # Exchange Data
            # "exchange_assets": FreshnessConfig("cg_exchange_assets", "updated_at", "datetime"),  # DISABLED
            # "exchange_balance_list": FreshnessConfig("cg_exchange_balance_list", "updated_at", "datetime"),  # DISABLED - Not documented
            # "exchange_onchain_transfers": FreshnessConfig("cg_exchange_onchain_transfers", "transaction_time"),  # DISABLED
            # Spot Market Data
            "spot_orderbook": FreshnessConfig("cg_spot_orderbook_history", "time"),
            "spot_orderbook_aggregated": FreshnessConfig(
                "cg_spot_orderbook_aggregated", "time"
            ),
            # "spot_supported_exchange_pairs": FreshnessConfig("cg_spot_supported_exchange_pairs", "updated_at", "datetime"),  # DISABLED - Reference data
            "spot_coins_markets": FreshnessConfig(
                "cg_spot_coins_markets", "updated_at", "datetime"
            ),
            "spot_pairs_markets": FreshnessConfig(
                "cg_spot_pairs_markets", "updated_at", "datetime"
            ),
            "spot_price_history": FreshnessConfig("cg_spot_price_history", "time"),
            # Bitcoin ETF Data
            "bitcoin_etf_list": FreshnessConfig(
                "cg_bitcoin_etf_list", "update_timestamp"
            ),
            # "bitcoin_etf_history": FreshnessConfig("cg_bitcoin_etf_history", "assets_date"),  # DISABLED - Endpoint not documented in API markdown
            "bitcoin_etf_premium_discount": FreshnessConfig(
                "cg_bitcoin_etf_premium_discount_history", "timestamp"
            ),
            # Macro Overlay Data
            "bitcoin_vs_global_m2_growth": FreshnessConfig(
                "cg_bitcoin_vs_global_m2_growth", "timestamp"
            ),
            # Options Data
            "option_exchange_oi_history": FreshnessConfig(
                "cg_option_exchange_oi_history", "updated_at", "datetime"
            ),
            # Open Interest Aggregated Stablecoin History Data
            "open_interest_aggregated_stablecoin_history": FreshnessConfig(
                "cg_open_interest_aggregated_stablecoin_history", "time"
            ),
            # Exchange Rank Data
            "exchange_rank": FreshnessConfig("cg_exchange_rank", "create_time"),
            # Sentiment Data
            "fear_greed_index": FreshnessConfig(
                "cg_fear_greed_index", "updated_at", "datetime"
            ),
            "hyperliquid_whale_alert": FreshnessConfig(
                "cg_hyperliquid_whale_alert", "create_time"
            ),
            "whale_transfer": FreshnessConfig("cg_whale_transfer", "block_timestamp"),
        }

        # Special expected ages for ETF data (updates less frequently)
        self.stream_configs["bitcoin_etf_list"].expected_max_age_hours = {
            FreshnessStatus.VERY_FRESH: 24.0,
            FreshnessStatus.FRESH: 48.0,
            FreshnessStatus.MODERATE: 72.0,
        }
        # self.stream_configs["bitcoin_etf_history"].expected_max_age_hours = {  # DISABLED - Endpoint not documented in API markdown
        #     FreshnessStatus.VERY_FRESH: 24.0,
        #     FreshnessStatus.FRESH: 48.0,
        #     FreshnessStatus.MODERATE: 72.0,
        # }
        self.stream_configs["bitcoin_etf_premium_discount"].expected_max_age_hours = {
            FreshnessStatus.VERY_FRESH: 24.0,
            FreshnessStatus.FRESH: 48.0,
            FreshnessStatus.MODERATE: 72.0,
        }

    def check_stream_freshness(self, stream_name: str) -> FreshnessResult:
        """Check freshness for a specific data stream."""
        if stream_name not in self.stream_configs:
            return FreshnessResult(
                stream_name=stream_name,
                status=FreshnessStatus.ERROR,
                total_records=0,
                latest_timestamp=None,
                latest_datetime=None,
                earliest_timestamp=None,
                hours_since_latest=None,
                records_last_24h=0,
                avg_records_per_hour=None,
                data_gaps_hours=[],
                error_message=f"Unknown stream: {stream_name}"
            )

        config = self.stream_configs[stream_name]
        now = datetime.now(wib_tz)

        try:
            with self.conn.cursor() as cur:
                # Get basic statistics
                stats_query = self._build_stats_query(config)
                cur.execute(stats_query)
                stats_row = cur.fetchone()

                if not stats_row or stats_row['total_records'] == 0:
                    return FreshnessResult(
                        stream_name=stream_name,
                        status=FreshnessStatus.NO_DATA,
                        total_records=0,
                        latest_timestamp=None,
                        latest_datetime=None,
                        earliest_timestamp=None,
                        hours_since_latest=None,
                        records_last_24h=0,
                        avg_records_per_hour=0,
                        data_gaps_hours=[],
                        recommendations=[f"No data found in {config.table_name}"]
                    )

                # Parse timestamps and calculate freshness
                latest_ts, earliest_ts = self._parse_timestamps(
                    stats_row['latest_timestamp'], stats_row['earliest_timestamp'], config
                )

                if latest_ts:
                    latest_dt = datetime.fromtimestamp(latest_ts / 1000, tz=wib_tz)
                    time_diff = now - latest_dt
                    hours_since_latest = time_diff.total_seconds() / 3600
                else:
                    latest_dt = None
                    hours_since_latest = None

                # Determine freshness status
                status = self._determine_freshness_status(hours_since_latest, config)

                # Get data continuity analysis (hourly gaps in last 24h)
                data_gaps_hours = self._analyze_data_continuity(cur, config, now)

                # Calculate average records per hour
                total_records = stats_row['total_records']
                if earliest_ts and latest_ts:
                    data_range_hours = (latest_ts - earliest_ts) / (1000 * 3600)
                    avg_records_per_hour = total_records / max(data_range_hours, 1)
                else:
                    avg_records_per_hour = None

                # Generate recommendations (will be called after getting cycle data)
                recommendations = []

                # Get records from latest cycle (last hour instead of 24h)
                records_this_cycle = self._get_records_this_cycle(cur, config, now)

                # Calculate cycle duration (time between recent data insertions)
                cycle_duration_minutes = self._calculate_cycle_duration(cur, config, now)

                # Generate recommendations with cycle data
                recommendations = self._generate_recommendations(
                    status, hours_since_latest, data_gaps_hours, records_this_cycle
                )

                return FreshnessResult(
                    stream_name=stream_name,
                    status=status,
                    total_records=total_records,
                    latest_timestamp=latest_ts,
                    latest_datetime=latest_dt.strftime('%d-%m-%Y %H:%M') if latest_dt else None,
                    earliest_timestamp=earliest_ts,
                    hours_since_latest=hours_since_latest,
                    records_last_24h=records_this_cycle,  # Reuse this field for cycle data
                    avg_records_per_hour=avg_records_per_hour,
                    data_gaps_hours=data_gaps_hours,
                    cycle_duration_minutes=cycle_duration_minutes,
                    recommendations=recommendations
                )

        except Exception as e:
            self.logger.error(f"Error checking freshness for {stream_name}: {e}", exc_info=True)
            return FreshnessResult(
                stream_name=stream_name,
                status=FreshnessStatus.ERROR,
                total_records=0,
                latest_timestamp=None,
                latest_datetime=None,
                earliest_timestamp=None,
                hours_since_latest=None,
                records_last_24h=0,
                avg_records_per_hour=None,
                data_gaps_hours=[],
                error_message=str(e)
            )

    def check_all_streams_freshness(self) -> Dict[str, FreshnessResult]:
        """Check freshness for all data streams."""
        results = {}
        for stream_name in self.stream_configs.keys():
            results[stream_name] = self.check_stream_freshness(stream_name)
        return results

    def log_freshness_status(self, results: Dict[str, FreshnessResult]) -> None:
        """Log freshness status for all streams with visual indicators."""
        self.logger.info("=" * 80)
        self.logger.info("📊 DATA FRESHNESS MONITORING REPORT")
        self.logger.info("=" * 80)

        now = datetime.now(wib_tz)
        self.logger.info(f"📅 Analysis Time: {now.strftime('%Y-%m-%d %H:%M:%S')} WIB")
        self.logger.info("")

        # Group by status
        status_groups = {
            FreshnessStatus.VERY_FRESH: [],
            FreshnessStatus.FRESH: [],
            FreshnessStatus.MODERATE: [],
            FreshnessStatus.STALE: [],
            FreshnessStatus.NO_DATA: [],
            FreshnessStatus.ERROR: [],
        }

        for stream_name, result in results.items():
            status_groups[result.status].append((stream_name, result))

        # Log by status category
        self._log_status_group(FreshnessStatus.VERY_FRESH, status_groups[FreshnessStatus.VERY_FRESH], "✅")
        self._log_status_group(FreshnessStatus.FRESH, status_groups[FreshnessStatus.FRESH], "✅")
        self._log_status_group(FreshnessStatus.MODERATE, status_groups[FreshnessStatus.MODERATE], "⚠️")
        self._log_status_group(FreshnessStatus.STALE, status_groups[FreshnessStatus.STALE], "❌")
        self._log_status_group(FreshnessStatus.NO_DATA, status_groups[FreshnessStatus.NO_DATA], "❌")
        self._log_status_group(FreshnessStatus.ERROR, status_groups[FreshnessStatus.ERROR], "❌")

        # Summary
        total_streams = len(results)
        healthy_streams = len(status_groups[FreshnessStatus.VERY_FRESH]) + len(status_groups[FreshnessStatus.FRESH])
        problematic_streams = total_streams - healthy_streams

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("📋 SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"📊 Total Streams: {total_streams}")
        self.logger.info(f"✅ Healthy Streams: {healthy_streams}")
        self.logger.info(f"⚠️  Problematic Streams: {problematic_streams}")

        if problematic_streams > 0:
            self.logger.info("")
            self.logger.info("🚨 REQUIRES ATTENTION:")
            for stream_name, result in results.items():
                if result.status in [FreshnessStatus.STALE, FreshnessStatus.NO_DATA, FreshnessStatus.ERROR]:
                    icon = self._get_status_icon(result.status)
                    self.logger.info(f"   {icon} {stream_name}: {result.error_message or self._get_status_description(result)}")

        self.logger.info("=" * 80)

    def get_freshness_alerts(self, results: Dict[str, FreshnessResult]) -> List[str]:
        """Generate alerts for streams that need attention."""
        alerts = []

        for stream_name, result in results.items():
            if result.status == FreshnessStatus.STALE:
                alerts.append(f"🚨 STALE DATA: {stream_name} - {result.hours_since_latest:.1f} hours old")
            elif result.status == FreshnessStatus.NO_DATA:
                alerts.append(f"🚨 NO DATA: {stream_name} - No records found")
            elif result.status == FreshnessStatus.ERROR:
                alerts.append(f"🚨 ERROR: {stream_name} - {result.error_message}")
            elif result.data_gaps_hours and len(result.data_gaps_hours) > 2:
                alerts.append(f"⚠️  DATA GAPS: {stream_name} - {len(result.data_gaps_hours)} hours with missing data")
            elif result.records_last_24h == 0 and result.status not in [FreshnessStatus.NO_DATA, FreshnessStatus.ERROR]:
                alerts.append(f"⚠️  NO CYCLE DATA: {stream_name} - No data in current cycle")

        return alerts

    def _build_stats_query(self, config: FreshnessConfig) -> str:
        """Build query to get basic statistics for a table."""
        if config.time_format == "datetime":
            # For datetime columns, convert to timestamp
            return f"""
                SELECT
                    COUNT(*) as total_records,
                    MAX(UNIX_TIMESTAMP({config.time_column}) * 1000) as latest_timestamp,
                    MIN(UNIX_TIMESTAMP({config.time_column}) * 1000) as earliest_timestamp
                FROM {config.table_name}
            """
        else:
            # For timestamp columns
            return f"""
                SELECT
                    COUNT(*) as total_records,
                    MAX({config.time_column}) as latest_timestamp,
                    MIN({config.time_column}) as earliest_timestamp
                FROM {config.table_name}
            """

    def _parse_timestamps(self, latest_ts, earliest_ts, config: FreshnessConfig) -> Tuple[Optional[int], Optional[int]]:
        """Parse timestamps based on format configuration."""
        if config.time_format == "datetime":
            # Already converted to timestamp_ms in query
            return latest_ts, earliest_ts
        elif config.time_format == "timestamp_s":
            # Convert seconds to milliseconds
            return latest_ts * 1000 if latest_ts else None, earliest_ts * 1000 if earliest_ts else None
        else:
            # Already in milliseconds
            return latest_ts, earliest_ts

    def _determine_freshness_status(self, hours_since_latest: Optional[float], config: FreshnessConfig) -> FreshnessStatus:
        """Determine freshness status based on hours since latest data."""
        if hours_since_latest is None:
            return FreshnessStatus.ERROR

        thresholds = config.expected_max_age_hours

        if hours_since_latest <= thresholds[FreshnessStatus.VERY_FRESH]:
            return FreshnessStatus.VERY_FRESH
        elif hours_since_latest <= thresholds[FreshnessStatus.FRESH]:
            return FreshnessStatus.FRESH
        elif hours_since_latest <= thresholds[FreshnessStatus.MODERATE]:
            return FreshnessStatus.MODERATE
        else:
            return FreshnessStatus.STALE

    def _get_records_this_cycle(self, cur, config: FreshnessConfig, now: datetime) -> int:
        """Get number of records in the latest cycle (last 1 hour)."""
        try:
            cutoff_timestamp = int((now - timedelta(hours=1)).timestamp() * 1000)

            if config.time_format == "datetime":
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {config.table_name}
                    WHERE UNIX_TIMESTAMP({config.time_column}) * 1000 >= {cutoff_timestamp}
                """
            else:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {config.table_name}
                    WHERE {config.time_column} >= {cutoff_timestamp}
                """

            cur.execute(query)
            row = cur.fetchone()
            return row['count'] if row else 0
        except Exception as e:
            self.logger.warning(f"Error getting cycle records for {config.table_name}: {e}")
            return 0

    def _calculate_cycle_duration(self, cur, config: FreshnessConfig, now: datetime) -> Optional[float]:
        """Calculate cycle duration based on time between recent data insertions."""
        try:
            # Get timestamps of the 2 most recent records
            if config.time_format == "datetime":
                query = f"""
                    SELECT UNIX_TIMESTAMP({config.time_column}) * 1000 as timestamp_ms
                    FROM {config.table_name}
                    ORDER BY {config.time_column} DESC
                    LIMIT 2
                """
            else:
                query = f"""
                    SELECT {config.time_column} as timestamp_ms
                    FROM {config.table_name}
                    ORDER BY {config.time_column} DESC
                    LIMIT 2
                """

            cur.execute(query)
            rows = cur.fetchall()

            if len(rows) >= 2:
                # Calculate time difference between consecutive records
                latest_ts = rows[0]['timestamp_ms']
                previous_ts = rows[1]['timestamp_ms']

                if latest_ts and previous_ts:
                    duration_minutes = (latest_ts - previous_ts) / (1000 * 60)
                    return max(0.1, min(duration_minutes, 60.0))  # Clamp between 0.1 and 60 minutes

            return None
        except Exception as e:
            self.logger.warning(f"Error calculating cycle duration for {config.table_name}: {e}")
            return None

    def _analyze_data_continuity(self, cur, config: FreshnessConfig, now: datetime) -> List[int]:
        """Analyze data continuity by checking for hourly gaps in last 24 hours."""
        try:
            gaps = []
            cutoff_timestamp = int((now - timedelta(hours=24)).timestamp() * 1000)

            for hours_ago in range(24):
                hour_start = int((now - timedelta(hours=hours_ago+1)).timestamp() * 1000)
                hour_end = int((now - timedelta(hours=hours_ago)).timestamp() * 1000)

                if config.time_format == "datetime":
                    query = f"""
                        SELECT COUNT(*) as count
                        FROM {config.table_name}
                        WHERE UNIX_TIMESTAMP({config.time_column}) * 1000 >= {hour_start}
                        AND UNIX_TIMESTAMP({config.time_column}) * 1000 < {hour_end}
                    """
                else:
                    query = f"""
                        SELECT COUNT(*) as count
                        FROM {config.table_name}
                        WHERE {config.time_column} >= {hour_start} AND {config.time_column} < {hour_end}
                    """

                cur.execute(query)
                row = cur.fetchone()
                count = row['count'] if row else 0

                if count == 0:
                    gaps.append(hours_ago)

            return gaps
        except Exception as e:
            self.logger.warning(f"Error analyzing continuity for {config.table_name}: {e}")
            return []

    def _generate_recommendations(self, status: FreshnessStatus, hours_since_latest: Optional[float],
                                 data_gaps_hours: List[int], records_last_24h: int) -> List[str]:
        """Generate recommendations based on freshness analysis."""
        recommendations = []

        if status == FreshnessStatus.STALE:
            recommendations.append(f"Data is {hours_since_latest:.1f} hours old - check pipeline")
        elif status == FreshnessStatus.NO_DATA:
            recommendations.append("No data found - check table creation and pipeline execution")
        elif status == FreshnessStatus.ERROR:
            recommendations.append("Error checking freshness - check database connection")

        if data_gaps_hours and len(data_gaps_hours) > 2:
            recommendations.append(f"Found {len(data_gaps_hours)} hourly gaps - check automation schedule")

        if records_last_24h == 0 and status != FreshnessStatus.NO_DATA:
            recommendations.append("No data in current cycle - check API connectivity")

        return recommendations

    def _log_status_group(self, status: FreshnessStatus, streams: List[Tuple[str, FreshnessResult]], icon: str):
        """Log a group of streams with the same status."""
        if not streams:
            return

        status_name = status.value.upper().replace("_", " ")
        self.logger.info(f"{icon} {status_name} ({len(streams)} streams):")

        for stream_name, result in streams:
            if result.latest_datetime:
                time_info = f"latest: {result.latest_datetime[:16]} WIB"
                if result.hours_since_latest is not None:
                    time_info += f" ({result.hours_since_latest:.1f}h ago)"
            else:
                time_info = "no timestamp"

            record_info = f"records: {result.total_records:,}"
            if result.records_last_24h > 0:
                record_info += f" (+{result.records_last_24h:,} this cycle"
                if result.cycle_duration_minutes is not None:
                    record_info += f" | {result.cycle_duration_minutes:.1f}min/cycle"
                record_info += ")"

            self.logger.info(f"   • {stream_name:<30} {time_info:<30} | {record_info}")

        self.logger.info("")

    def _get_status_icon(self, status: FreshnessStatus) -> str:
        """Get icon for freshness status."""
        icons = {
            FreshnessStatus.VERY_FRESH: "✅",
            FreshnessStatus.FRESH: "✅",
            FreshnessStatus.MODERATE: "⚠️",
            FreshnessStatus.STALE: "❌",
            FreshnessStatus.NO_DATA: "❌",
            FreshnessStatus.ERROR: "❌",
        }
        return icons.get(status, "❓")

    def _get_status_description(self, result: FreshnessResult) -> str:
        """Get human-readable description of freshness status."""
        if result.status == FreshnessStatus.STALE:
            return f"Data is {result.hours_since_latest:.1f} hours old"
        elif result.status == FreshnessStatus.NO_DATA:
            return "No records found"
        elif result.status == FreshnessStatus.ERROR:
            return result.error_message or "Error checking freshness"
        else:
            return f"Data is {result.status.value.replace('_', ' ')}"
