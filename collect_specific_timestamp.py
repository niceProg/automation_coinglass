#!/usr/bin/env python3
"""
Specific Data Collection Script for Timestamp 1704067200000 (2024-01-01 00:00:00 UTC)

This script retrieves data from three specific Coinglass endpoints:
1. cg_open_interest_aggregated_history
2. cg_spot_aggregated_taker_volume_history
3. cg_spot_aggregated_ask_bids_history

All data will be collected starting from timestamp 1704067200000.
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.config import Settings
from app.database.connection import get_connection
from app.providers.coinglass.client import CoinglassClient
from app.repositories.coinglass_repository import CoinglassRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Target timestamp
TARGET_TIMESTAMP = 1704067200000  # 2024-01-01 00:00:00 UTC

# Configuration
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"]
EXCHANGES = ["Binance", "OKX", "Bybit"]  # Individual exchanges for processing
INTERVALS = ["1h", "4h", "12h", "1d"]


def collect_open_interest_aggregated_history(client, repo):
    """Collect open interest aggregated history data."""
    logger.info("🔄 Collecting Open Interest Aggregated History...")

    total_saved = 0
    total_duplicates = 0
    total_fetches = 0

    for symbol in SYMBOLS:
        for interval in INTERVALS:
            try:
                total_fetches += 1
                logger.info(f"Fetching OI data for {symbol} at {interval} interval...")

                rows = client.get_oi_aggregated_history(
                    symbol=symbol,
                    interval=interval,
                    start_time=TARGET_TIMESTAMP,
                    unit="usd"
                )

                if rows:
                    result = repo.upsert_oi_aggregated_history(
                        symbol=symbol,
                        interval=interval,
                        rows=rows,
                        unit="usd"
                    )

                    saved = result.get("oi_aggregated_history", 0)
                    duplicates = result.get("oi_aggregated_history_duplicates", 0)

                    total_saved += saved
                    total_duplicates += duplicates

                    logger.info(f"✅ OI {symbol} {interval}: {saved} saved, {duplicates} duplicates, {len(rows)} received")
                else:
                    logger.warning(f"⚠️ No data received for OI {symbol} {interval}")

            except Exception as e:
                logger.error(f"❌ Error collecting OI data for {symbol} {interval}: {e}")
                continue

    logger.info(f"📊 OI Summary: {total_saved} saved, {total_duplicates} duplicates, {total_fetches} fetches")
    return {"saved": total_saved, "duplicates": total_duplicates, "fetches": total_fetches}


def collect_spot_aggregated_taker_volume_history(client, repo):
    """Collect spot aggregated taker volume history data."""
    logger.info("🔄 Collecting Spot Aggregated Taker Volume History...")

    total_saved = 0
    total_duplicates = 0
    total_fetches = 0

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    total_fetches += 1
                    logger.info(f"Fetching taker volume data for {exchange} {symbol} at {interval} interval...")

                    rows = client.get_spot_aggregated_taker_volume_history(
                        exchange_list=exchange,  # Single exchange name
                        symbol=symbol,
                        interval=interval,
                        start_time=TARGET_TIMESTAMP,
                        unit="usd"
                    )

                    if rows:
                        result = repo.insert_spot_aggregated_taker_volume_history(
                            exchange, symbol, interval, "usd", rows
                        )

                        saved = result.get("saved", 0)
                        duplicates = result.get("duplicates", 0)

                        total_saved += saved
                        total_duplicates += duplicates

                        logger.info(f"✅ Taker Volume {exchange} {symbol} {interval}: {saved} saved, {duplicates} duplicates, {len(rows)} received")
                    else:
                        logger.warning(f"⚠️ No data received for taker volume {exchange} {symbol} {interval}")

                except Exception as e:
                    logger.error(f"❌ Error collecting taker volume data for {exchange} {symbol} {interval}: {e}")
                    continue

    logger.info(f"📊 Taker Volume Summary: {total_saved} saved, {total_duplicates} duplicates, {total_fetches} fetches")
    return {"saved": total_saved, "duplicates": total_duplicates, "fetches": total_fetches}


def collect_spot_aggregated_ask_bids_history(client, repo):
    """Collect spot aggregated ask/bids history data."""
    logger.info("🔄 Collecting Spot Aggregated Ask/Bids History...")

    total_saved = 0
    total_duplicates = 0
    total_fetches = 0

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    total_fetches += 1
                    logger.info(f"Fetching ask/bids data for {exchange} {symbol} at {interval} interval...")

                    rows = client.get_spot_aggregated_ask_bids_history(
                        exchange_list=exchange,  # Single exchange name
                        symbol=symbol,
                        interval=interval,
                        start_time=TARGET_TIMESTAMP,
                        range_percent="1"  # 1% depth
                    )

                    if rows:
                        result = repo.upsert_spot_aggregated_ask_bids_history_batch(
                            exchange, symbol, interval, "1", rows
                        )

                        saved = result.get("spot_aggregated_ask_bids_history", 0)
                        duplicates = result.get("spot_aggregated_ask_bids_history_duplicates", 0)

                        total_saved += saved
                        total_duplicates += duplicates

                        logger.info(f"✅ Ask/Bids {exchange} {symbol} {interval}: {saved} saved, {duplicates} duplicates, {len(rows)} received")
                    else:
                        logger.warning(f"⚠️ No data received for ask/bids {exchange} {symbol} {interval}")

                except Exception as e:
                    logger.error(f"❌ Error collecting ask/bids data for {exchange} {symbol} {interval}: {e}")
                    continue

    logger.info(f"📊 Ask/Bids Summary: {total_saved} saved, {total_duplicates} duplicates, {total_fetches} fetches")
    return {"saved": total_saved, "duplicates": total_duplicates, "fetches": total_fetches}


def main():
    """Main function to collect data from all three endpoints."""
    logger.info("🚀 Starting specific data collection for timestamp 1704067200000 (2024-01-01 00:00:00 UTC)")

    try:
        # Initialize database connection and API client
        logger.info("📡 Connecting to database and API...")
        conn = get_connection()
        client = CoinglassClient()
        repo = CoinglassRepository(conn, logger)

        # Ensure database schema exists
        logger.info("🔧 Ensuring database schema...")
        repo.ensure_schema()

        # Collect data from all three endpoints
        results = {
            "open_interest": collect_open_interest_aggregated_history(client, repo),
            "taker_volume": collect_spot_aggregated_taker_volume_history(client, repo),
            "ask_bids": collect_spot_aggregated_ask_bids_history(client, repo)
        }

        # Print final summary
        logger.info("🎉 Data collection completed!")
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY:")
        logger.info(f"Open Interest Aggregated History:")
        logger.info(f"  - Saved: {results['open_interest']['saved']}")
        logger.info(f"  - Duplicates: {results['open_interest']['duplicates']}")
        logger.info(f"  - Fetches: {results['open_interest']['fetches']}")

        logger.info(f"Spot Aggregated Taker Volume History:")
        logger.info(f"  - Saved: {results['taker_volume']['saved']}")
        logger.info(f"  - Duplicates: {results['taker_volume']['duplicates']}")
        logger.info(f"  - Fetches: {results['taker_volume']['fetches']}")

        logger.info(f"Spot Aggregated Ask/Bids History:")
        logger.info(f"  - Saved: {results['ask_bids']['saved']}")
        logger.info(f"  - Duplicates: {results['ask_bids']['duplicates']}")
        logger.info(f"  - Fetches: {results['ask_bids']['fetches']}")

        total_saved = sum(r['saved'] for r in results.values())
        total_duplicates = sum(r['duplicates'] for r in results.values())
        total_fetches = sum(r['fetches'] for r in results.values())

        logger.info(f"TOTAL:")
        logger.info(f"  - Records Saved: {total_saved}")
        logger.info(f"  - Duplicates: {total_duplicates}")
        logger.info(f"  - API Fetches: {total_fetches}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ Fatal error in data collection: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("🔌 Database connection closed")


if __name__ == "__main__":
    main()