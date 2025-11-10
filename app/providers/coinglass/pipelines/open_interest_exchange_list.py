import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Open Interest Exchange List Pipeline
    Cadence: Every 1 hour
    Endpoint: /api/futures/open-interest/exchange-list

    Retrieves open interest information from various exchanges for specific symbols,
    including total open interest, margin breakdown, and percentage changes over
    multiple timeframes.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    SYMBOLS = params.get("symbols", settings.COINGLASS_SYMBOLS)

    summary = {
        "open_interest_exchange_list": 0,
        "fetches": 0
    }

    logger.info(f"Starting Futures Open Interest Exchange List pipeline for symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        try:
            logger.info(f"Fetching open interest exchange list for {symbol}")
            rows = client.get_open_interest_exchange_list(symbol=symbol)

            if rows:
                saved = repo.upsert_open_interest_exchange_list(symbol, rows)
                summary["open_interest_exchange_list"] += saved
                logger.info(f"✅ open_interest_exchange_list[{symbol}]: received={len(rows)}, saved={saved} ✅")
            else:
                logger.warning(f"⚠️ open_interest_exchange_list[{symbol}]: No data returned")

            summary["fetches"] += 1

        except Exception as e:
            logger.warning(f"⚠️ open_interest_exchange_list[{symbol}]: Exception: {e} (skipped)")
            summary["fetches"] += 1
            continue

    logger.info(
        f"📦 Futures Open Interest Exchange List pipeline completed. Total records saved: {summary['open_interest_exchange_list']} ✅ | "
        f"open_interest_exchange_list:{summary['open_interest_exchange_list']} (fetches:{summary['fetches']})"
    )
    return summary