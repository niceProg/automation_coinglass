#!/usr/bin/env python3
"""
Coinglass Automation - Main CLI

COMMAND CATEGORIES:

🔧 SYSTEM ADMINISTRATION:
    --setup                          Setup database tables and schema
    --status                         Check ingestion status and record counts
    --freshness                      Check data freshness for all pipelines

📊 DATA COLLECTION MODES:
    --continuous                     Run continuous automation (10s intervals)
    --dev                            Run development mode (10s intervals)
    --server                         Run server automation mode (1s intervals)
    --initial-scrape --months N      Fetch N months of historical data

📈 DERIVATIVES MARKET:
    funding_rate                     OHLC funding rate data (8h + 1h snapshots)
    # oi_history                        Open interest OHLC history by exchange/symbol/interval [DISABLED]
        oi_aggregated_history            Open interest OHLC data aggregated across exchanges
    long_short_ratio_global          Global account ratios (all intervals)
    long_short_ratio_top             Top account ratios (all intervals)
    liquidation_aggregated           Coin liquidations aggregated across exchanges (BTC, ETH, SOL)
    liquidation_heatmap              Liquidation heatmap visualization (BTC, ETH, SOL, XRP, HYPE, BNB, DOGE)
    futures_basis                    Futures basis data (BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, HYPEUSDT, BNBUSDT, DOGEUSDT)
    futures_footprint_history        Futures volume footprint data with taker buy/sell volumes at price ranges
    options                          Options market data and Max Pain

🏦 EXCHANGE INFRASTRUCTURE:
    # exchange_assets                  Exchange wallet balances and assets (DISABLED)
    # exchange_balance_list            Exchange balance changes over time (DISABLED - Not documented)
    # exchange_onchain_transfers       On-chain transfer monitoring (DISABLED)

💰 SPOT MARKET:
    spot_coins_markets               Comprehensive coin market data
    spot_pairs_markets               Trading pair market data
    spot_price_history               Historical OHLC price data
    spot_large_orderbook             Current large limit orders in spot markets
    spot_large_orderbook_history     Historical data for large limit orders
    spot_aggregated_taker_volume_history  Aggregated taker buy/sell volumes across exchanges
    spot_taker_volume_history        Single exchange taker buy/sell volume data
    spot_ask_bids_history            Historical orderbook bid/ask data with depth ranges
    spot_aggregated_ask_bids_history  Aggregated orderbook bid/ask data across exchanges

₿ BITCOIN ETF:
    bitcoin_etf_list                 Bitcoin ETF overview and status (real-time)
    # bitcoin_etf_history              Historical holdings and NAV data  # DISABLED - Endpoint not documented
    bitcoin_etf_flows_history        ETF flows and capital movements
    bitcoin_etf_premium_discount_history  Premium/discount tracking

📊 TRADING MARKET:
    # supported_exchange_pairs         Supported trading pairs reference [DISABLED]
    # pairs_markets                    Trading pair market data [DISABLED]
    # coins_markets                    Cryptocurrency market data [DISABLED]

📊 MACRO OVERLAY:
    bitcoin_vs_global_m2_growth      Bitcoin vs Global M2 supply and growth data

📊 OPTIONS:
    option_exchange_oi_history       Options exchange open interest history

😱 SENTIMENT & ON-CHAIN:
    fear_greed_index                 Crypto fear & greed index
    hyperliquid_whale_alert          Hyperliquid whale position alerts
    whale_transfer                   Large on-chain transfers ($10M+)

🔬 CRYPTOQUANT ANALYTICS:
    exchange_inflow_cdd              Exchange Inflow CDD (Coin Days Destroyed) data

Usage Examples:
    # System Administration
    python main.py --setup
    python main.py --status
    python main.py --freshness

    # Data Collection
    python main.py --initial-scrape --months 12
    python main.py --continuous
    python main.py --dev
    python main.py --server

    # Historical Data Collection
    python main.py --historical 3        # 3 years of historical data (all time-based pipelines)
    python main.py --historical 1        # 1 year of historical data
    python main.py --historical 1672531200 1763164800000  # Custom timestamp range
    python main.py --historical 1672531200  # From specific date to now
    python main.py --historical 3 funding_rate spot_ask_bids_history  # Historical data for specific pipelines

    # Individual Pipelines
    python main.py funding_rate oi_aggregated_history long_short_ratio_global long_short_ratio_top liquidation_aggregated liquidation_heatmap futures_basis futures_footprint_history
    # python main.py exchange_balance_list  # DISABLED - Not documented
    python main.py spot_coins_markets spot_pairs_markets spot_price_history spot_large_orderbook spot_large_orderbook_history spot_aggregated_taker_volume_history spot_taker_volume_history spot_ask_bids_history spot_aggregated_ask_bids_history
    python main.py bitcoin_etf_list bitcoin_etf_flows_history  # bitcoin_etf_history disabled
    # python main.py supported_exchange_pairs pairs_markets  # DISABLED
"""

import argparse
import sys
import time
import logging
import platform
import os
from datetime import datetime
from app.controllers.ingestion_controller import IngestionController

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def setup_database():
    """Setup database tables."""
    logger.info("=" * 60)
    logger.info("SETUP: Creating database tables")
    logger.info("=" * 60)

    controller = IngestionController()
    result = controller.setup_database()

    if result.get("status") == "success":
        logger.info("✅ Database setup completed successfully")
        return True
    else:
        logger.error(f"❌ Database setup failed: {result.get('message')}")
        return False


def initial_scrape(months: int = 1):
    """Run initial historical data scrape."""
    logger.info("=" * 60)
    logger.info(f"INITIAL SCRAPE: Fetching {months} month(s) of historical data")
    logger.info("=" * 60)

    controller = IngestionController()
    results = controller.run_initial_scrape(months=months)

    # Print summary with duplicate detection
    total_fresh = 0
    total_duplicates = 0
    total_processed = 0

    logger.info("📊 INITIAL SCRAPE SUMMARY:")
    logger.info("-" * 60)
    for pipeline, result in results.items():
        if "error" in result:
            logger.error(f"❌ {pipeline}: {result['error']}")
        else:
            # Calculate fresh vs duplicate data
            pipeline_fresh = 0
            pipeline_duplicates = 0

            for key, value in result.items():
                if isinstance(value, int) and not key.endswith("_duplicates") and not key.endswith("_filtered"):
                    pipeline_fresh += value
                elif key.endswith("_duplicates"):
                    pipeline_duplicates += value

            pipeline_total = pipeline_fresh + pipeline_duplicates
            total_fresh += pipeline_fresh
            total_duplicates += pipeline_duplicates
            total_processed += pipeline_total

            # Display detailed breakdown
            if pipeline_duplicates > 0:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records, {pipeline_duplicates} duplicates")
            else:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records")

    # Overall summary
    logger.info("-" * 60)
    if total_duplicates > 0:
        if total_fresh > 0:
            logger.info(f"📈 INITIAL SCRAPE TOTAL: {total_fresh} fresh records saved, {total_duplicates} duplicates found")
            logger.info(f"📊 Freshness Rate: {(total_fresh/total_processed*100):.1f}% ({total_fresh}/{total_processed})")
        else:
            logger.info(f"📈 INITIAL SCRAPE TOTAL: {total_duplicates} duplicates found (no fresh records)")
            logger.info(f"📊 All data already exists in database")
    else:
        logger.info(f"📈 INITIAL SCRAPE TOTAL: {total_fresh} fresh records saved")

    logger.info("=" * 60)

    return results


def run_pipelines(pipelines=None):
    """Run specific pipelines or all pipelines."""
    if pipelines:
        logger.info("=" * 60)
        logger.info(f"RUNNING PIPELINES: {', '.join(pipelines)}")
        logger.info("=" * 60)
    else:
        logger.info("=" * 60)
        logger.info("RUNNING ALL PIPELINES")
        logger.info("=" * 60)

    controller = IngestionController()
    results = controller.run_coinglass(pipelines=pipelines)

    # Track overall statistics
    total_fresh = 0
    total_duplicates = 0
    total_processed = 0

    # Print summary with duplicate detection
    logger.info("📊 PIPELINE EXECUTION SUMMARY:")
    logger.info("-" * 60)
    for pipeline, result in results.items():
        if "error" in result:
            logger.error(f"❌ {pipeline}: {result['error']}")
        else:
            # Calculate fresh vs duplicate data
            pipeline_fresh = 0
            pipeline_duplicates = 0

            for key, value in result.items():
                # Count only fresh records (non-duplicate keys, exclude fetches and duplicate keys)
                if isinstance(value, int) and key != "fetches" and not key.endswith("_duplicates") and not key.endswith("_filtered"):
                    pipeline_fresh += value
                elif key.endswith("_duplicates"):
                    pipeline_duplicates += value

            pipeline_total = pipeline_fresh + pipeline_duplicates
            total_fresh += pipeline_fresh
            total_duplicates += pipeline_duplicates
            total_processed += pipeline_total

            # Display detailed breakdown
            if pipeline_duplicates > 0:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records, {pipeline_duplicates} duplicates")
            else:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records")

    # Overall summary
    logger.info("-" * 60)
    if total_duplicates > 0:
        if total_fresh > 0:
            logger.info(f"📈 OVERALL: {total_fresh} fresh records saved, {total_duplicates} duplicates found")
            logger.info(f"📊 Freshness Rate: {(total_fresh/total_processed*100):.1f}% ({total_fresh}/{total_processed})")
        else:
            logger.info(f"📈 OVERALL: {total_duplicates} duplicates found (no fresh records)")
            logger.info(f"📊 All data already exists in database")
    else:
        logger.info(f"📈 OVERALL: {total_fresh} fresh records saved")

    logger.info("=" * 60)

    return results


def continuous_mode(dev_mode: bool = False, server_mode: bool = False, pipelines: list = None):
    """Run continuous data collection with automation every 10 seconds or 1 second for server mode."""
    # Log current date and time when starting
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H:%M:%S')

    if server_mode:
        mode_name = "Server automation mode"
        cycle_delay = 1
    else:
        mode_name = "Development mode" if dev_mode else "Automation mode"
        cycle_delay = 10

    cycle_count = 0

    logger.info("=" * 60)
    logger.info(f"CONTINUOUS MODE: {mode_name} (delay: {cycle_delay}s)")
    logger.info("=" * 60)
    logger.info(f"📅 Current Date: {date_str}")
    logger.info(f"🕐 Current Time: {time_str}")
    logger.info(f"🔄 Starting continuous mode (delay: {cycle_delay}s)...")

    if pipelines:
        logger.info(f"📊 Pipelines: {', '.join(pipelines)}")
    else:
        logger.info("📊 Pipelines: All Coinglass pipelines")

    # Run pipelines at specified intervals
    logger.info("📅 Automation schedule:")
    if pipelines:
        logger.info(f"   - Specified pipelines: Every {cycle_delay} second(s)")
    else:
        logger.info(f"   - All pipelines: Every {cycle_delay} second(s)")
    logger.info("   - Retrieves updated data from Coinglass API")
    if server_mode:
        logger.info("   - Optimized for Docker server deployment")
        logger.info("   - High-frequency data collection")
    logger.info("=" * 60)

    # Run initial collection
    logger.info("Running initial collection...")
    run_pipelines(pipelines=pipelines)

    # Start automation loop
    logger.info(f"🔄 {mode_name} active. Press Ctrl+C to stop.")
    logger.info("=" * 60)

    try:
        while True:
            cycle_count += 1

            # Update date and time for each cycle
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            logger.info("=" * 60)
            logger.info(f"🔄 CYCLE #{cycle_count} - {date_str} {time_str}")
            logger.info("=" * 60)

            run_pipelines(pipelines=pipelines)

            logger.info(f"✅ Cycle #{cycle_count} completed. Next cycle in {cycle_delay}s...")
            time.sleep(cycle_delay)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info(f"⏹️  {mode_name} stopped after {cycle_count} cycles")
        logger.info("=" * 60)


def show_status():
    """Show ingestion status."""
    logger.info("=" * 60)
    logger.info("INGESTION STATUS")
    logger.info("=" * 60)

    controller = IngestionController()
    status = controller.get_status()

    if "error" in status:
        logger.error(f"❌ Failed to get status: {status['error']}")
        return

    for key, data in status.items():
        logger.info(f"\n{key.upper()}:")
        logger.info(f"  Total Records: {data.get('count', 0):,}")
        logger.info(f"  Latest Time: {data.get('latest_datetime', 'N/A')}")

    logger.info("\n" + "=" * 60)


def show_freshness():
    """Show data freshness status for all pipelines."""
    logger.info("=" * 80)
    logger.info("🔍 DATA FRESHNESS ANALYSIS")
    logger.info("=" * 80)

    controller = IngestionController()
    results = controller.check_freshness()

    if "error" in results:
        logger.error(f"❌ Failed to check freshness: {results['error']}")
        return

    logger.info("=" * 80)
    logger.info("✅ Freshness analysis completed")
    logger.info("=" * 80)


def create_time_batches(start_ts, end_ts, batch_days=30):
    """Create time batches for historical data retrieval to avoid API limits."""
    from datetime import datetime, timedelta
    import math

    batches = []
    current_start = start_ts
    batch_duration = batch_days * 24 * 60 * 60  # Convert days to seconds

    while current_start < end_ts:
        current_end = min(current_start + batch_duration, end_ts)
        batches.append({
            'start_time': current_start * 1000,  # Convert to milliseconds for API
            'end_time': current_end * 1000
        })
        current_start = current_end

    return batches


def run_historical_mode(historical_args, pipelines=None):
    """Run historical data collection with custom time parameters and batch processing."""
    from app.services.coinglass_service import CoinglassService
    from datetime import datetime, timedelta
    import logging

    logger = logging.getLogger(__name__)

    # Parse historical arguments
    start_time = None
    end_time = None
    years_ago = None  # Track years for error messages

    if len(historical_args) == 0:
        # Default to 1 year if no arguments provided
        years = 1
        years_ago = years
        end_time = datetime.now()
        start_time = end_time - timedelta(days=365 * years)
    elif len(historical_args) == 1:
        # Preset mode: --historical 3 (3 years)
        try:
            years = int(historical_args[0])
            years_ago = years
            end_time = datetime.now()
            start_time = end_time - timedelta(days=365 * years)
        except ValueError:
            # If not a number, treat as start timestamp
            try:
                start_time = datetime.fromtimestamp(int(historical_args[0]))
                end_time = datetime.now()
            except (ValueError, OSError):
                logger.error(f"❌ Invalid timestamp format: {historical_args[0]}")
                return
    elif len(historical_args) >= 2:
        # Manual mode: --historical start_time end_time
        try:
            start_time = datetime.fromtimestamp(int(historical_args[0]))
            end_time = datetime.fromtimestamp(int(historical_args[1]))
        except (ValueError, OSError) as e:
            logger.error(f"❌ Invalid timestamp format: {e}")
            return

    # Convert to Unix timestamps for API
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())

    # Apply Coinglass API constraints
    min_timestamp = 1704067200000 // 1000  # January 1, 2024 in seconds
    if start_timestamp < min_timestamp:
        logger.info(f"⚠️ Adjusting start_time from {start_timestamp} to {min_timestamp} (January 1, 2024) due to API limitations")
        start_timestamp = min_timestamp
        start_time = datetime.fromtimestamp(min_timestamp)

    logger.info("=" * 80)
    logger.info("📅 HISTORICAL DATA COLLECTION MODE")
    logger.info("=" * 80)
    logger.info(f"🕐 Time Range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🔢 Timestamps: {start_timestamp} to {end_timestamp}")

    # Define intervals that support batch processing (1 hour and above)
    batch_timeframes = ["1h", "4h", "6h", "8h", "12h", "1d", "1w"]
    all_time_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"]

    # Create time batches for historical data
    time_batches = create_time_batches(start_timestamp, end_timestamp, batch_days=30)
    logger.info(f"📦 Created {len(time_batches)} time batches (30-day chunks)")
    for i, batch in enumerate(time_batches):
        start_dt = datetime.fromtimestamp(batch['start_time'] // 1000)
        end_dt = datetime.fromtimestamp(batch['end_time'] // 1000)
        logger.info(f"   Batch {i+1}: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")

    # Default pipelines if none specified - only pipelines that support time-based parameters
    if not pipelines:
        pipelines = [
            # Pipelines with "timeframes" parameter
            "funding_rate",
            "oi_aggregated_history",
            "long_short_ratio_global",
            "long_short_ratio_top",
            "liquidation_aggregated",
            "futures_basis",
            # Pipelines with "intervals" parameter
            "spot_price_history",
            "open_interest_aggregated_stablecoin_history",
            "futures_footprint_history",
            "spot_large_orderbook_history",
            "spot_aggregated_taker_volume_history",
            "spot_taker_volume_history",
            "spot_ask_bids_history",
            "spot_aggregated_ask_bids_history",
            # Pipelines with direct time parameter support
            "whale_transfer"
        ]
        logger.info(f"📊 Using default pipelines: {', '.join(pipelines)}")

    logger.info(f"🎯 Running pipelines: {', '.join(pipelines)}")
    logger.info(f"⚡ Batch processing enabled for timeframes: {', '.join(batch_timeframes)}")
    logger.info("=" * 80)

    # Run with batch processing
    try:
        service = CoinglassService(ensure_tables=False)
        results = {}

        for pipeline_name in pipelines:
            if pipeline_name not in service.pipelines:
                logger.warning(f"⚠️ {pipeline_name}: Not found in service pipelines")
                continue

            logger.info(f"\n🔄 Running {pipeline_name} with batch processing...")
            pipeline_total = {"records": 0, "batches": 0, "errors": 0}

            # Process each time batch
            for batch_idx, batch in enumerate(time_batches):
                logger.info(f"   📦 Processing batch {batch_idx + 1}/{len(time_batches)} "
                           f"({datetime.fromtimestamp(batch['start_time'] // 1000).strftime('%Y-%m-%d')} to "
                           f"{datetime.fromtimestamp(batch['end_time'] // 1000).strftime('%Y-%m-%d')})")

                # Merge batch params with pipeline default params
                pipeline_params = service.pipelines[pipeline_name]["params"].copy()

                # Apply batch time parameters
                batch_params = {
                    "start_time": batch['start_time'],
                    "end_time": batch['end_time']
                }
                pipeline_params.update(batch_params)

                # Filter timeframes/intervals based on batch processing rules
                if "intervals" in pipeline_params:
                    original_intervals = pipeline_params["intervals"]
                    if isinstance(original_intervals, list):
                        # For batch processing: use all intervals for non-batch timeframes, filter for batch timeframes
                        filtered_intervals = [interval for interval in original_intervals
                                            if interval in all_time_intervals]
                        pipeline_params["intervals"] = filtered_intervals
                        logger.info(f"      📝 Intervals: {pipeline_params['intervals']}")
                elif "timeframes" in pipeline_params:
                    original_timeframes = pipeline_params["timeframes"]
                    if isinstance(original_timeframes, list):
                        # For batch processing: only use 1h+ timeframes
                        filtered_timeframes = [timeframe for timeframe in original_timeframes
                                             if timeframe in batch_timeframes]
                        pipeline_params["timeframes"] = filtered_timeframes
                        logger.info(f"      📝 Timeframes (batch-only): {pipeline_params['timeframes']}")
                else:
                    # Pipeline doesn't use intervals/timeframes (e.g., whale_transfer)
                    # Time parameters are already applied via batch_params
                    logger.info(f"      📝 Direct time parameters applied (no intervals/timeframes)")

                try:
                    result = service.pipelines[pipeline_name]["func"](
                        conn=service.conn,
                        client=service.client,
                        params=pipeline_params
                    )

                    # Count records from this batch
                    if isinstance(result, dict):
                        batch_records = sum(v for k, v in result.items()
                                          if isinstance(v, int) and not str(k).endswith("_duplicates"))
                    else:
                        batch_records = int(result) if isinstance(result, (int, float)) else 0

                    pipeline_total["records"] += batch_records
                    pipeline_total["batches"] += 1

                    logger.info(f"      ✅ Batch {batch_idx + 1} completed: {batch_records} records")

                except Exception as pipeline_error:
                    pipeline_total["errors"] += 1
                    # Special handling for funding_rate time errors
                    if pipeline_name == "funding_rate" and "time error" in str(pipeline_error):
                        logger.warning(f"      ⚠️ Batch {batch_idx + 1}: API time limitation (expected for old data)")
                    else:
                        logger.error(f"      ❌ Batch {batch_idx + 1}: {pipeline_error}")

            results[pipeline_name] = pipeline_total
            logger.info(f"✅ {pipeline_name} completed: {pipeline_total['records']} total records "
                       f"({pipeline_total['batches']} batches, {pipeline_total['errors']} errors)")

        logger.info("=" * 80)
        logger.info("📊 HISTORICAL COLLECTION SUMMARY")
        logger.info("=" * 80)

        total_records = 0
        total_batches = 0
        total_errors = 0

        for pipeline_name, result in results.items():
            if isinstance(result, dict) and "records" in result:
                total_records += result["records"]
                total_batches += result["batches"]
                total_errors += result["errors"]

                if result["errors"] > 0:
                    logger.info(f"✅ {pipeline_name}: {result['records']} records ({result['batches']} batches, {result['errors']} errors)")
                else:
                    logger.info(f"✅ {pipeline_name}: {result['records']} records ({result['batches']} batches)")
            else:
                logger.info(f"✅ {pipeline_name}: Completed")

        # Overall summary
        logger.info("-" * 80)
        logger.info(f"📈 OVERALL: {total_records} total records collected")
        logger.info(f"📦 Processed: {total_batches} batches across {len(pipelines)} pipelines")
        if total_errors > 0:
            logger.info(f"⚠️ Errors: {total_errors} batch failures (may be expected due to API limitations)")
        logger.info("=" * 80)
        logger.info("🎉 Historical data collection completed successfully!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ Failed to run historical collection: {e}")
        logger.info("=" * 80)
        raise


def show_help():
    """Show help information when no arguments are provided."""
    now = datetime.now()
    session_id = f"session_{now.strftime('%Y%m%d_%H%M%S')}"

    logger.info("=" * 80)
    logger.info("🚀 COINGLASS AUTOMATION CLI")
    logger.info("=" * 80)
    logger.info(f"🆔 Session ID: {session_id}")
    logger.info(f"🕐 Start Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🖥️  Working Directory: {os.getcwd()}")
    logger.info(f"🐍 Python Version: {platform.python_version()} ({platform.system()} {platform.release()})")

    logger.info("=" * 80)
    logger.info("📋 COMMAND CATEGORIES")
    logger.info("=" * 80)

    # System Administration
    logger.info("\n🔧 SYSTEM ADMINISTRATION:")
    logger.info("  --setup                     Setup database tables and schema")
    logger.info("  --status                    Show ingestion status and record counts")
    logger.info("  --freshness                 Check data freshness for all pipelines")

    # Data Collection Modes
    logger.info("\n📊 DATA COLLECTION MODES:")
    logger.info("  --continuous                Run continuous automation (10s intervals)")
    logger.info("  --dev                       Run development mode (10s intervals)")
    logger.info("  --server                    Run server automation mode (1s intervals)")
    logger.info("  --initial-scrape --months N  Fetch N months of historical data")

    # Derivatives Market
    logger.info("\n📈 DERIVATIVES MARKET:")

    # Open Interest Info Logger
    logger.info("\n🔍 OPEN INTEREST ANALYSIS:")
    logger.info("  📊 Comprehensive Open Interest Data Coverage:")
    logger.info("     • Current OI snapshots by exchange and symbol")
    logger.info("     • Historical OI OHLC data across multiple timeframes")
    logger.info("     • Aggregated OI data across multiple exchanges")
    logger.info("     • Stablecoin margin OI analysis (USDT/USDC backed positions)")
    logger.info("     • OI metrics: Open interest value, quantity, and rankings")
    logger.info("     • Exchange rankings by OI volume and market share")

    logger.info("  funding_rate                OHLC funding rate data (8h + 1h snapshots)")
    # logger.info("  oi_history                  Open interest OHLC history by exchange/symbol/interval [DISABLED]")
    logger.info("  oi_aggregated_history      Open interest OHLC data aggregated across exchanges")
    logger.info("  open_interest_aggregated_stablecoin_history  Aggregated stablecoin margin OHLC data")
    logger.info("  long_short_ratio_global      Global account ratios (all intervals)")
    logger.info("  long_short_ratio_top         Top account ratios (all intervals)")
    logger.info(
        "  liquidation_aggregated       Coin liquidations aggregated (all intervals)"
    )
    logger.info("  liquidation_heatmap          Liquidation heatmap visualization")
    logger.info("  futures_basis                Futures basis data (all intervals)")
    # logger.info("  options                     Options market data and Max Pain [DISABLED]")

    # Exchange Infrastructure
    # logger.info("\n🏦 EXCHANGE INFRASTRUCTURE:")
    # logger.info("  exchange_assets             Exchange wallet balances and assets")
    # logger.info("  exchange_balance_list       Exchange balance changes over time [DISABLED - Not documented]")
    # logger.info("  exchange_onchain_transfers  On-chain transfer monitoring")

    # Spot Market
    logger.info("\n💰 SPOT MARKET:")
    logger.info("  spot_coins_markets          Comprehensive coin market data")
    logger.info("  spot_pairs_markets          Trading pair market data")
    logger.info(
        "  spot_price_history          Historical OHLC price data (all mandatory pairs)"
    )

    # Bitcoin ETF
    logger.info("\n₿ BITCOIN ETF:")
    logger.info("  bitcoin_etf_list            Bitcoin ETF overview and status (real-time)")
    # logger.info("  bitcoin_etf_history         Historical holdings and NAV data")  # DISABLED - Endpoint not documented
    logger.info("  bitcoin_etf_flows_history    ETF flows and capital movements")
    logger.info("  bitcoin_etf_premium_discount_history  Premium/discount tracking")

    # Trading Market
    # logger.info("\n📊 TRADING MARKET:")
    # logger.info("  supported_exchange_pairs    Supported trading pairs reference")
    # logger.info("  pairs_markets               Trading pair market data")
    # logger.info("  coins_markets               Cryptocurrency market data [DISABLED]")

    # Macro Overlay
    logger.info("\n📊 MACRO OVERLAY:")
    logger.info(
        "  bitcoin_vs_global_m2_growth  Bitcoin vs Global M2 supply and growth data"
    )

    # Options
    logger.info("\n📊 OPTIONS:")
    logger.info("  option_exchange_oi_history   Options exchange open interest history")

    # Sentiment & On-chain
    logger.info("\n😱 SENTIMENT & ON-CHAIN:")
    logger.info("  fear_greed_index             Crypto fear & greed index")
    logger.info("  hyperliquid_whale_alert      Hyperliquid whale position alerts")
    logger.info("  whale_transfer               Large on-chain transfers ($10M+)")

    logger.info("=" * 80)
    logger.info("💡 USAGE EXAMPLES")
    logger.info("=" * 80)

    logger.info("\n🔧 System Administration:")
    logger.info("  python main.py --setup")
    logger.info("  python main.py --status")
    logger.info("  python main.py --freshness")

    logger.info("\n📊 Data Collection:")
    logger.info("  python main.py --initial-scrape --months 12")
    logger.info("  python main.py --continuous")
    logger.info("  python main.py --dev")
    logger.info("  python main.py --server    # High-frequency (1s) for Docker deployment")

    logger.info("\n📅 Historical Data Collection:")
    logger.info("  python main.py --historical 3        # 3 years of historical data")
    logger.info("  python main.py --historical 1        # 1 year of historical data")
    logger.info("  python main.py --historical 1672531200 1763164800000  # Custom timestamp range")
    logger.info("  python main.py --historical 1672531200  # From specific date to now")
    logger.info("  python main.py --historical 3 funding_rate spot_ask_bids_history  # Specific pipelines")

    logger.info("\n📈 Individual Market Categories:")
    logger.info("  # Derivatives")
    logger.info(
        "  python main.py funding_rate oi_aggregated_history long_short_ratio_global long_short_ratio_top liquidation_aggregated liquidation_heatmap futures_basis futures_footprint_history"
    )
    # logger.info("  python main.py funding_rate open_interest long_short_ratio options [options DISABLED]")
    logger.info("  ")
    # logger.info("  # Exchange Infrastructure")
    # logger.info("  python main.py exchange_balance_list")  # DISABLED - Not documented
    logger.info("  ")
    logger.info("  # Spot Microstructure")
    logger.info("  python main.py spot_coins_markets spot_pairs_markets spot_price_history")
    logger.info("  python main.py spot_large_orderbook spot_large_orderbook_history")
    logger.info("  python main.py spot_aggregated_taker_volume_history spot_taker_volume_history")
    logger.info("  python main.py spot_ask_bids_history spot_aggregated_ask_bids_history")
    logger.info("  ")
    logger.info("  # Bitcoin ETFs")
    logger.info(
        "  python main.py bitcoin_etf_list bitcoin_etf_flows_history bitcoin_etf_premium_discount_history  # bitcoin_etf_history disabled"
    )
    logger.info("  ")
    logger.info("  # Macro Overlay")
    logger.info("  python main.py bitcoin_vs_global_m2_growth")
    logger.info("  ")
    logger.info("  # Options")
    logger.info("  python main.py option_exchange_oi_history")
    logger.info("  ")
    logger.info("  # Sentiment & On-chain")
    logger.info(
        "  python main.py fear_greed_index hyperliquid_whale_alert whale_transfer"
    )

    logger.info("\n📖 Additional Information:")
    logger.info("  Use --help for detailed parameter information")
    logger.info("  All pipelines support individual execution")
    logger.info("  Continuous mode runs all pipelines every 10 seconds")
    logger.info("  Server mode runs all pipelines every 1 second (Docker optimized)")
    logger.info("  Server mode is recommended for production Docker deployments")

    logger.info("=" * 80)
    logger.info("✅ HELP INFORMATION DISPLAYED")
    logger.info(f"🕐 End Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🆔 Session ID: {session_id}")
    logger.info("=" * 80)


def run_cryptoquant_pipelines(pipelines=None):
    """Run specific CryptoQuant pipelines or all CryptoQuant pipelines."""
    if pipelines:
        logger.info("=" * 60)
        logger.info(f"RUNNING CRYPTOQUANT PIPELINES: {', '.join(pipelines)}")
        logger.info("=" * 60)
    else:
        logger.info("=" * 60)
        logger.info("RUNNING ALL CRYPTOQUANT PIPELINES")
        logger.info("=" * 60)

    controller = IngestionController()
    results = controller.run_cryptoquant(pipelines=pipelines)

    # Track overall statistics
    total_fresh = 0
    total_duplicates = 0
    total_processed = 0

    # Print summary with duplicate detection
    logger.info("📊 CRYPTOQUANT PIPELINE EXECUTION SUMMARY:")
    logger.info("-" * 60)
    for pipeline, result in results.items():
        if "error" in result:
            logger.error(f"❌ {pipeline}: {result['error']}")
        else:
            # Calculate fresh vs duplicate data
            pipeline_fresh = 0
            pipeline_duplicates = 0

            for key, value in result.items():
                # Count only fresh records (non-duplicate keys, exclude fetches and duplicate keys)
                if isinstance(value, int) and key != "fetches" and not key.endswith("_duplicates") and not key.endswith("_filtered"):
                    pipeline_fresh += value
                elif key.endswith("_duplicates"):
                    pipeline_duplicates += value

            pipeline_total = pipeline_fresh + pipeline_duplicates
            total_fresh += pipeline_fresh
            total_duplicates += pipeline_duplicates
            total_processed += pipeline_total

            # Display detailed breakdown
            if pipeline_duplicates > 0:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records, {pipeline_duplicates} duplicates")
            else:
                logger.info(f"✅ {pipeline}: {pipeline_fresh} fresh records")

    # Overall summary
    logger.info("-" * 60)
    if total_duplicates > 0:
        if total_fresh > 0:
            logger.info(f"📈 OVERALL: {total_fresh} fresh records saved, {total_duplicates} duplicates found")
            logger.info(f"📊 Freshness Rate: {(total_fresh/total_processed*100):.1f}% ({total_fresh}/{total_processed})")
        else:
            logger.info(f"📈 OVERALL: {total_duplicates} duplicates found (no fresh records)")
            logger.info(f"📊 All data already exists in database")
    else:
        logger.info(f"📈 OVERALL: {total_fresh} fresh records saved")

    logger.info("=" * 60)

    return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Coinglass Automation CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--setup", action="store_true", help="Setup database tables"
    )
    parser.add_argument(
        "--initial-scrape",
        action="store_true",
        help="Run initial historical data scrape",
    )
    parser.add_argument(
        "--months", type=int, default=1, help="Number of months for initial scrape (default: 1)"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="Run continuous automation (10s intervals)"
    )
    parser.add_argument(
        "--dev", action="store_true", help="Run development mode (10s intervals)"
    )
    parser.add_argument(
        "--server", action="store_true", help="Run server automation mode (1s intervals)"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show ingestion status"
    )
    parser.add_argument(
        "--freshness", action="store_true", help="Check data freshness for all pipelines"
    )
    parser.add_argument(
        "--historical",
        nargs="*",
        help="Run historical data collection with custom time ranges:\n"
        "Preset: --historical 1 year, 2 years, 3 years, etc.\n"
        "Manual: --historical 1672531200 1763164800000 (start_time end_time)"
    )
    parser.add_argument(
        "pipelines",
        nargs="*",
        help="Specific pipelines to run by category:\n"
        "Derivatives: funding_rate, oi_aggregated_history, long_short_ratio_global, long_short_ratio_top, liquidation_aggregated, liquidation_heatmap, futures_basis, futures_footprint_history\n"
        "Exchange: exchange_assets [DISABLED], exchange_balance_list [DISABLED], exchange_onchain_transfers [DISABLED]\n"
        "Spot: spot_coins_markets, spot_pairs_markets, spot_price_history, spot_large_orderbook, spot_large_orderbook_history, spot_aggregated_taker_volume_history, spot_taker_volume_history, spot_ask_bids_history, spot_aggregated_ask_bids_history\n"
        "Bitcoin ETF: bitcoin_etf_list, bitcoin_etf_flows_history, bitcoin_etf_premium_discount_history\n"
        "Trading: supported_exchange_pairs [DISABLED], pairs_markets [DISABLED], coins_markets [DISABLED]\n"
        "Macro: bitcoin_vs_global_m2_growth\n"
        "Options: option_exchange_oi_history\n"
        "Sentiment: fear_greed_index, hyperliquid_whale_alert, whale_transfer\n"
        "CryptoQuant: exchange_inflow_cdd",
    )

    args = parser.parse_args()

    # Handle commands
    if args.setup:
        success = setup_database()
        sys.exit(0 if success else 1)

    elif args.initial_scrape:
        results = initial_scrape(months=args.months)
        # Check if any pipeline failed
        has_errors = any("error" in r for r in results.values())
        sys.exit(1 if has_errors else 0)

    elif args.continuous:
        continuous_mode(dev_mode=False, server_mode=False, pipelines=args.pipelines if args.pipelines else None)

    elif args.dev:
        continuous_mode(dev_mode=True, server_mode=False, pipelines=args.pipelines if args.pipelines else None)

    elif args.server:
        continuous_mode(dev_mode=False, server_mode=True, pipelines=args.pipelines if args.pipelines else None)

    elif args.status:
        show_status()

    elif args.freshness:
        show_freshness()

    elif args.historical is not None:
        run_historical_mode(args.historical, args.pipelines if args.pipelines else None)

    elif args.pipelines:
        # Check if any CryptoQuant pipelines are requested
        cryptoquant_pipelines = ["exchange_inflow_cdd"]
        requested_cryptoquant = [p for p in args.pipelines if p in cryptoquant_pipelines]
        requested_coinglass = [p for p in args.pipelines if p not in cryptoquant_pipelines]

        results = {}

        # Run Coinglass pipelines if any
        if requested_coinglass:
            coinglass_results = run_pipelines(pipelines=requested_coinglass)
            results.update(coinglass_results)

        # Run CryptoQuant pipelines if any
        if requested_cryptoquant:
            cryptoquant_results = run_cryptoquant_pipelines(pipelines=requested_cryptoquant)
            results.update(cryptoquant_results)

        has_errors = any("error" in r for r in results.values())
        sys.exit(1 if has_errors else 0)

    else:
        # No arguments - show help information
        show_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
