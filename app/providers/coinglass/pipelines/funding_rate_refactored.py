"""Refactored Funding Rate Pipeline using base classes."""
from typing import Any, Dict, List
from app.providers.coinglass.pipelines.base import OHLCDataPipeline, PipelineConfig
from app.core.utils import RateLimiter


class FundingRatePipeline(OHLCDataPipeline):
    """Pipeline for fetching and storing funding rate data."""

    def __init__(self, client: Any, logger: Any):
        """Initialize funding rate pipeline."""
        config = PipelineConfig(
            name="funding_rate",
            cadence="8h",
            required_params=["symbols"],
            default_params={
                "timeframes": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"],
                "fetch_method": "get_fr_history",
                "save_method": "upsert_fr_history",
                "is_time_series": True,
                "group_by": ["exchange", "pair", "interval"],
                "batch_size": 5
            }
        )

        # Rate limiter: 10 requests per second
        rate_limiter = RateLimiter(calls=10, period=1.0)

        super().__init__(config, client, logger=logger, rate_limiter=rate_limiter)


def run(connection: Any, client: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Run the funding rate pipeline."""
    logger = __import__("logging").getLogger(__name__)
    pipeline = FundingRatePipeline(client, logger)
    return pipeline.run(connection, client, params)
