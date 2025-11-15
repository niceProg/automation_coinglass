# app/providers/coinglass/pipelines/whale_transfer.py
import logging
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Whale Transfer Pipeline
    Fetches large on-chain transfers (minimum $10M) within specified time range.
    Supports start_time and end_time parameters for historical data collection.
    """
    repo = CoinglassRepository(conn, logger)

    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "DOGE"])

    # Extract time parameters if available
    time_params = {}
    if "start_time" in params:
        time_params["start_time"] = params["start_time"]
    if "end_time" in params:
        time_params["end_time"] = params["end_time"]

    summary = {
        "whale_transfer": 0,
        "whale_transfer_duplicates": 0,
        "fetches": 0
    }

    # Fetch all transfers (no symbol filter)
    try:
        rows = client.get_chain_whale_transfer(**time_params)
        if rows:
            result = repo.upsert_whale_transfer(rows)
            saved = result.get("whale_transfer", 0)
            duplicates = result.get("whale_transfer_duplicates", 0)

            time_info = f" (time range: {time_params.get('start_time', 'default')}-{time_params.get('end_time', 'default')})" if time_params else ""
            logger.info(
                f"✅ whale_transfer[ALL]: "
                f"received={len(rows)}, saved={saved}, duplicates={duplicates}{time_info}"
            )
            summary["whale_transfer"] += saved
            summary["whale_transfer_duplicates"] += duplicates
        else:
            logger.info("⚠️ whale_transfer[ALL]: No data (skipped)")
        summary["fetches"] += 1
    except Exception as e:
        logger.warning(f"⚠️ whale_transfer[ALL]: Exception: {e} (skipped)")
        summary["fetches"] += 1

    # Fetch symbol-specific transfers
    for symbol in SYMBOLS:
        try:
            rows = client.get_chain_whale_transfer(symbol=symbol, **time_params)
            if rows:
                result = repo.upsert_whale_transfer(rows)
                saved = result.get("whale_transfer", 0)
                duplicates = result.get("whale_transfer_duplicates", 0)

                time_info = f" (time range: {time_params.get('start_time', 'default')}-{time_params.get('end_time', 'default')})" if time_params else ""
                logger.info(
                    f"✅ whale_transfer[{symbol}]: "
                    f"received={len(rows)}, saved={saved}, duplicates={duplicates}{time_info}"
                )
                summary["whale_transfer"] += saved
                summary["whale_transfer_duplicates"] += duplicates
            else:
                logger.info(f"⚠️ whale_transfer[{symbol}]: No data (skipped)")
            summary["fetches"] += 1
        except Exception as e:
            logger.warning(f"⚠️ whale_transfer[{symbol}]: Exception: {e} (skipped)")
            summary["fetches"] += 1
            continue

    logger.info(
        f"📦 Whale Transfer summary -> "
        f"saved={summary['whale_transfer']}, "
        f"duplicates={summary['whale_transfer_duplicates']}, "
        f"fetches={summary['fetches']}"
    )

    return summary
