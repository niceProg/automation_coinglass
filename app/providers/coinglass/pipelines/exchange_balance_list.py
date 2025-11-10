import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exchange Balance List Pipeline
    Cadence: Every 1 hour
    Endpoints:
    - /api/exchange/balance/list

    Retrieves balance information for exchanges across different symbols
    including total balances and balance changes over various timeframes.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    SYMBOLS = params.get("symbols", settings.COINGLASS_SYMBOLS)

    summary = {
        "exchange_balance_list": 0,
        "fetches": 0
    }

    logger.info(f"Starting Exchange Balance List pipeline for symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        try:
            logger.info(f"Fetching exchange balance list for {symbol}")
            rows = client.get_exchange_balance_list(symbol=symbol)

            if rows:
                saved = repo.upsert_exchange_balance_list(symbol, rows)
                summary["exchange_balance_list"] += saved
                logger.info(f"✅ exchange_balance_list[{symbol}]: received={len(rows)}, saved={saved} ✅")
            else:
                logger.warning(f"⚠️ exchange_balance_list[{symbol}]: No data returned")

            summary["fetches"] += 1

        except Exception as e:
            logger.warning(f"⚠️ exchange_balance_list[{symbol}]: Exception: {e} (skipped)")
            summary["fetches"] += 1
            continue

    logger.info(
        f"📦 Exchange Balance List pipeline completed. Total records saved: {summary['exchange_balance_list']} ✅ | "
        f"exchange_balance_list:{summary['exchange_balance_list']} (fetches:{summary['fetches']})"
    )
    return summary