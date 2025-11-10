# app/providers/coinglass/pipelines/liquidation_heatmap.py
import logging
import time
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Liquidation Aggregated Heatmap Pipeline
    Cadence: Real-time for Professional/Enterprise plans
    Time ranges: 12h, 24h, 3d, 7d, 30d, 90d, 180d, 1y

    Collects liquidation heatmap data for coins.
    Note: Only available on Professional and Enterprise plans.
    Note: Includes 20-second delay between API calls to respect rate limits.
    """
    repo = CoinglassRepository(conn, logger)

    # Liquidation heatmap specific ranges
    RANGES = params.get("ranges", ["12h", "24h", "3d", "7d", "30d", "90d", "180d", "1y"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])

    summary = {
        "liquidation_heatmap": 0,
        "liquidation_heatmap_duplicates": 0,
        "liquidation_heatmap_fetches": 0
    }

    # Liquidation Aggregated Heatmap
    for symbol in SYMBOLS:
        for range_param in RANGES:
            try:
                # Add 20-second delay between API calls with countdown
                delay_seconds = 20
                logger.info(f"⏳ Waiting {delay_seconds} seconds before next request...")
                for remaining in range(delay_seconds, 0, -1):
                    if remaining % 5 == 0 or remaining <= 3:
                        logger.info(f"⏱️  {remaining} seconds remaining...")
                    time.sleep(1)
                logger.info("✓ Delay complete, proceeding with request")

                data = client.get_liquidation_aggregated_heatmap(
                    symbol=symbol, range_param=range_param
                )
                if data:
                    result = repo.upsert_liquidation_heatmap(
                        symbol=symbol, range_param=range_param, data=data
                    )
                    saved = result.get("liquidation_heatmap", 0)
                    duplicates = result.get("liquidation_heatmap_duplicates", 0)

                    logger.info(
                        f"✅ liquidation_heatmap[{symbol}:{range_param}]: "
                        f"saved={saved}, duplicates={duplicates}"
                    )
                    summary["liquidation_heatmap"] += saved
                    summary["liquidation_heatmap_duplicates"] += duplicates
                else:
                    logger.info(
                        f"⚠️ liquidation_heatmap[{symbol}:{range_param}]: No data (skipped)"
                    )
                summary["liquidation_heatmap_fetches"] += 1
            except Exception as e:
                logger.warning(
                    f"⚠️ liquidation_heatmap[{symbol}:{range_param}]: Exception: {e} (skipped)"
                )
                summary["liquidation_heatmap_fetches"] += 1
                continue

    logger.info(
        f"📦 Liquidation Heatmap summary -> total_saved={summary['liquidation_heatmap']} | "
        f"liquidation_heatmap:{summary['liquidation_heatmap']} (duplicates:{summary['liquidation_heatmap_duplicates']}) "
        f"(fetches:{summary['liquidation_heatmap_fetches']})"
    )

    return summary