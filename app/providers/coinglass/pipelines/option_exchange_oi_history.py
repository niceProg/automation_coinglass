# app/providers/coinglass/pipelines/option_exchange_oi_history.py
import logging
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Option Exchange OI History Pipeline
    Fetches open interest history for options.
    """
    repo = CoinglassRepository(conn, logger)

    SYMBOLS = params.get("symbols", ["BTC", "ETH"])
    UNITS = params.get("units", ["USD"])
    RANGES = params.get("ranges", ["1h", "4h", "12h", "all"])

    summary = {
        "option_exchange_oi_history": 0,
        "option_exchange_oi_history_duplicates": 0,
        "fetches": 0
    }

    for symbol in SYMBOLS:
        for unit in UNITS:
            for range_param in RANGES:
                try:
                    data = client.get_option_exchange_oi_history(
                        symbol=symbol, unit=unit, range_param=range_param
                    )
                    if data:
                        result = repo.upsert_option_exchange_oi_history(
                            symbol=symbol, unit=unit, range_param=range_param, data=data
                        )
                        saved = result.get("option_exchange_oi_history", 0)
                        duplicates = result.get("option_exchange_oi_history_duplicates", 0)

                        logger.info(
                            f"✅ option_exchange_oi_history[{symbol}:{unit}:{range_param}]: "
                            f"saved={saved}, duplicates={duplicates}"
                        )
                        summary["option_exchange_oi_history"] += saved
                        summary["option_exchange_oi_history_duplicates"] += duplicates
                    else:
                        logger.info(
                            f"⚠️ option_exchange_oi_history[{symbol}:{unit}:{range_param}]: No data (skipped)"
                        )
                    summary["fetches"] += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ option_exchange_oi_history[{symbol}:{unit}:{range_param}]: Exception: {e} (skipped)"
                    )
                    summary["fetches"] += 1
                    continue

    logger.info(
        f"📦 Option Exchange OI History summary -> "
        f"saved={summary['option_exchange_oi_history']}, "
        f"duplicates={summary['option_exchange_oi_history_duplicates']}, "
        f"fetches={summary['fetches']}"
    )

    return summary
