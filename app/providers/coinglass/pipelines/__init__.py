# app/providers/coinglass/pipelines/__init__.py
from . import (
    funding_rate,
    # open_interest,  # DISABLED - Replaced by separate OI pipelines
    # oi_history,  # DISABLED
    oi_aggregated_history,
    long_short_ratio_global,
    long_short_ratio_top,
    liquidation_aggregated,
    liquidation_heatmap,
    futures_basis,
    # options,  # DISABLED
    # exchange_assets,  # DISABLED
    # exchange_balance_list,  # DISABLED - Not documented
    # exchange_onchain_transfers,  # DISABLED
    spot_orderbook,
    spot_orderbook_aggregated,
    spot_coins_markets,
    spot_pairs_markets,
    spot_price_history,
    # ===== NEW ENDPOINTS =====
    futures_footprint_history,
    spot_large_orderbook_history,
    spot_large_orderbook,
    spot_aggregated_taker_volume_history,
    spot_taker_volume_history,
    bitcoin_etf_list,
    # bitcoin_etf_history,  # DISABLED - Endpoint not documented in API markdown
    bitcoin_etf_flows_history,
    bitcoin_etf_premium_discount_history,
    bitcoin_vs_global_m2_growth,
    option_exchange_oi_history,
    # open_interest_exchange_list,  # DISABLED - Replaced by oi_exchange_list
    open_interest_aggregated_stablecoin_history,
    # coins_markets,  # DISABLED
    fear_greed_index,
    hyperliquid_whale_alert,
    whale_transfer,
)

__all__ = [
    "funding_rate",
    # "open_interest",  # DISABLED - Replaced by separate OI pipelines
    # "oi_history",  # DISABLED
    "oi_aggregated_history",
    "long_short_ratio_global",
    "long_short_ratio_top",
    "liquidation_aggregated",
    "liquidation_heatmap",
    "futures_basis",
    # "options",  # DISABLED
    # "exchange_assets",  # DISABLED
    # "exchange_balance_list",  # DISABLED - Not documented
    # "exchange_onchain_transfers",  # DISABLED
    "spot_orderbook",
    "spot_orderbook_aggregated",
    "spot_coins_markets",
    "spot_pairs_markets",
    "spot_price_history",
    # ===== NEW ENDPOINTS =====
    "futures_footprint_history",
    "spot_large_orderbook_history",
    "spot_large_orderbook",
    "spot_aggregated_taker_volume_history",
    "spot_taker_volume_history",
    "bitcoin_etf_list",
    # "bitcoin_etf_history",  # DISABLED - Endpoint not documented in API markdown
    "bitcoin_etf_flows_history",
    "bitcoin_etf_premium_discount_history",
    "bitcoin_vs_global_m2_growth",
    "option_exchange_oi_history",
    # "open_interest_exchange_list",  # DISABLED - Replaced by oi_exchange_list
    "open_interest_aggregated_stablecoin_history",
    # "coins_markets",  # DISABLED
    "fear_greed_index",
    "hyperliquid_whale_alert",
    "whale_transfer",
]
