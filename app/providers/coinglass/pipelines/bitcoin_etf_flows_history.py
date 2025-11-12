import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bitcoin ETF Flows History Pipeline
    Cadence: Every 1 hour (historical data)
    Endpoint: /api/etf/bitcoin/flow-history

    Retrieves historical flow data for Bitcoin ETFs including daily net inflows/outflows
    and individual ETF flow breakdowns.
    """
    repo = CoinglassRepository(conn, logger)

    summary = {
        "bitcoin_etf_flows": 0,
        "bitcoin_etf_flows_duplicates": 0,
        "bitcoin_etf_flows_details": 0,
        "bitcoin_etf_flows_details_duplicates": 0,
        "fetches": 0
    }

    logger.info("Starting Bitcoin ETF Flows History pipeline")

    try:
        logger.info("Fetching ETF flows history")
        rows = client.get_etf_flows_history()

        if rows:
            # Log sample data for debugging
            if len(rows) > 0:
                sample_row = rows[0]
                logger.info(f"Sample ETF flows data: timestamp={sample_row.get('timestamp')}, "
                           f"flow_usd={sample_row.get('flow_usd')}, "
                           f"etf_flows_count={len(sample_row.get('etf_flows', []))}")

            result = repo.upsert_bitcoin_etf_flows_history(rows)
            flows_saved = result.get("bitcoin_etf_flows", 0)
            flows_duplicates = result.get("bitcoin_etf_flows_duplicates", 0)
            flows_received = result.get("bitcoin_etf_flows_received", 0)

            details_saved = result.get("bitcoin_etf_flows_details", 0)
            details_duplicates = result.get("bitcoin_etf_flows_details_duplicates", 0)
            details_received = result.get("bitcoin_etf_flows_details_received", 0)

            summary["bitcoin_etf_flows"] += flows_saved
            summary["bitcoin_etf_flows_duplicates"] += flows_duplicates
            summary["bitcoin_etf_flows_details"] += details_saved
            summary["bitcoin_etf_flows_details_duplicates"] += details_duplicates

            logger.info(
                f"✅ Flows history: received={flows_received}, saved={flows_saved}, duplicates={flows_duplicates} | "
                f"Flows details: received={details_received}, saved={details_saved}, duplicates={details_duplicates} ✅"
            )
        else:
            logger.warning("No data returned for ETF flows history")

        summary["fetches"] += 1

    except Exception as e:
        logger.warning(f"Error fetching ETF flows history: {e}")
        summary["fetches"] += 1

    logger.info(
        f"📦 Bitcoin ETF Flows History pipeline completed. "
        f"Flows: saved={summary['bitcoin_etf_flows']} (duplicates: {summary['bitcoin_etf_flows_duplicates']}) | "
        f"Details: saved={summary['bitcoin_etf_flows_details']} (duplicates: {summary['bitcoin_etf_flows_details_duplicates']}) ✅"
    )
    return summary