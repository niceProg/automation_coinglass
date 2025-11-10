import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bitcoin ETF Premium/Discount History Pipeline
    Cadence: Every 1 hour (historical data)
    Endpoint: /api/etf/bitcoin/premium-discount/history

    Retrieves historical premium/discount data across multiple Bitcoin ETFs
    showing NAV vs market price comparisons over time.

    Note: The API returns a nested structure with timestamp and list of ETFs.
    The client flattens this to individual records with ticker included.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters - optional specific ticker filter
    TICKER = params.get("ticker", None)  # If None, returns all ETFs

    summary = {
        "bitcoin_etf_premium_discount": 0,
        "bitcoin_etf_premium_discount_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Bitcoin ETF Premium/Discount History pipeline for ticker: {TICKER or 'ALL'}")

    try:
        logger.info(f"Fetching ETF premium/discount history for {TICKER or 'all ETFs'}")
        rows = client.get_etf_premium_discount_history(TICKER)

        if rows:
            # Log sample data for debugging
            if len(rows) > 0:
                sample_row = rows[0]
                logger.info(f"Sample ETF premium/discount data: timestamp={sample_row.get('timestamp')}, "
                           f"ticker={sample_row.get('ticker')}, nav_usd={sample_row.get('nav_usd')}, "
                           f"market_price_usd={sample_row.get('market_price_usd')}, "
                           f"premium_discount_details={sample_row.get('premium_discount_details')}")

            # Don't pass ticker parameter since it's now in the row data
            result = repo.upsert_bitcoin_etf_premium_discount_history(rows, ticker=None)
            saved = result.get("bitcoin_etf_premium_discount", 0)
            duplicates = result.get("bitcoin_etf_premium_discount_duplicates", 0)

            summary["bitcoin_etf_premium_discount"] += saved
            summary["bitcoin_etf_premium_discount_duplicates"] += duplicates

            # Count unique tickers in response
            unique_tickers = set(row.get("ticker") for row in rows if row.get("ticker"))
            ticker_count = len(unique_tickers)
            ticker_label = TICKER if TICKER else f"ALL ({ticker_count} tickers)"
            logger.info(
                f"✅ ETF premium/discount [{ticker_label}]: received={len(rows)}, "
                f"saved={saved}, duplicates={duplicates} ✅"
            )
        else:
            logger.warning(f"No data returned for ETF premium/discount history: {TICKER or 'ALL'}")

        summary["fetches"] += 1

    except Exception as e:
        logger.warning(f"Error fetching ETF premium/discount history: {e}")
        summary["fetches"] += 1

    logger.info(
        f"📦 Bitcoin ETF Premium/Discount History pipeline completed. Total records saved: {summary['bitcoin_etf_premium_discount']} "
        f"(duplicates: {summary['bitcoin_etf_premium_discount_duplicates']}) ✅"
    )
    return summary