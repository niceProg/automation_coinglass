# app/models/coinglass.py

COINGLASS_TABLES = {
    # ----- Funding Rate Tables -----
    "cg_funding_rate_history": """
    CREATE TABLE IF NOT EXISTS cg_funding_rate_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        pair VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open DECIMAL(18,8),
        high DECIMAL(18,8),
        low DECIMAL(18,8),
        close DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_pair (pair),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "cg_funding_rate_exchange_list": """
    CREATE TABLE IF NOT EXISTS cg_funding_rate_exchange_list (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        exchange VARCHAR(50) NOT NULL,
        margin_type VARCHAR(20) NOT NULL,
        funding_rate DECIMAL(18,8),
        funding_rate_interval INT,
        next_funding_time BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_exchange_margin (symbol, exchange, margin_type),
        INDEX idx_symbol (symbol),
        INDEX idx_exchange (exchange)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

  
    "cg_open_interest_aggregated_history": """
    CREATE TABLE IF NOT EXISTS cg_open_interest_aggregated_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open DECIMAL(38,8),
        high DECIMAL(38,8),
        low DECIMAL(38,8),
        close DECIMAL(38,8),
        unit VARCHAR(10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_interval_time (symbol, `interval`, time),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time),
        INDEX idx_unit (unit)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ===== DISABLED TABLES (Commented Out) =====

    # ----- Trading Market Tables (DISABLED) -----
    # "cg_supported_exchange_pairs": """
    # CREATE TABLE IF NOT EXISTS cg_supported_exchange_pairs (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     exchange_name VARCHAR(50) NOT NULL,
    #     instrument_id VARCHAR(100) NOT NULL,
    #     base_asset VARCHAR(20) NOT NULL,
    #     quote_asset VARCHAR(20) NOT NULL,
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_exchange_instrument (exchange_name, instrument_id),
    #     INDEX idx_exchange_name (exchange_name),
    #     INDEX idx_base_asset (base_asset),
    #     INDEX idx_quote_asset (quote_asset),
    #     INDEX idx_instrument_id (instrument_id)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # "cg_pairs_markets": """
    # CREATE TABLE IF NOT EXISTS cg_pairs_markets (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     instrument_id VARCHAR(100) NOT NULL,
    #     exchange_name VARCHAR(50) NOT NULL,
    #     symbol VARCHAR(50) NOT NULL,
    #     current_price DECIMAL(30,10),
    #     index_price DECIMAL(30,10),
    #     price_change_percent_24h DECIMAL(18,8),
    #     volume_usd DECIMAL(38,8),
    #     volume_usd_change_percent_24h DECIMAL(18,8),
    #     long_volume_usd DECIMAL(38,8),
    #     short_volume_usd DECIMAL(38,8),
    #     long_volume_quantity DECIMAL(38,8),
    #     short_volume_quantity DECIMAL(38,8),
    #     open_interest_quantity DECIMAL(38,8),
    #     open_interest_usd DECIMAL(38,8),
    #     open_interest_change_percent_24h DECIMAL(18,8),
    #     long_liquidation_usd_24h DECIMAL(38,8),
    #     short_liquidation_usd_24h DECIMAL(38,8),
    #     funding_rate DECIMAL(18,8),
    #     next_funding_time BIGINT,
    #     open_interest_volume_radio DECIMAL(18,8),
    #     oi_vol_ratio_change_percent_24h DECIMAL(18,8),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_instrument_exchange (instrument_id, exchange_name),
    #     INDEX idx_exchange_name (exchange_name),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_current_price (current_price),
    #     INDEX idx_volume_usd (volume_usd),
    #     INDEX idx_open_interest_usd (open_interest_usd)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # "cg_coins_markets": """
    # CREATE TABLE IF NOT EXISTS cg_coins_markets (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     symbol VARCHAR(20) NOT NULL,
    #     current_price DECIMAL(30,10),
    #     avg_funding_rate_by_oi DECIMAL(18,8),
    #     avg_funding_rate_by_vol DECIMAL(18,8),
    #     market_cap_usd DECIMAL(38,8),
    #     open_interest_market_cap_ratio DECIMAL(18,8),
    #     open_interest_usd DECIMAL(38,8),
    #     open_interest_quantity DECIMAL(38,8),
    #     open_interest_volume_ratio DECIMAL(18,8),
    #     price_change_percent_5m DECIMAL(18,8),
    #     price_change_percent_15m DECIMAL(18,8),
    #     price_change_percent_30m DECIMAL(18,8),
    #     price_change_percent_1h DECIMAL(18,8),
    #     price_change_percent_4h DECIMAL(18,8),
    #     price_change_percent_12h DECIMAL(18,8),
    #     price_change_percent_24h DECIMAL(18,8),
    #     open_interest_change_percent_5m DECIMAL(18,8),
    #     open_interest_change_percent_15m DECIMAL(18,8),
    #     open_interest_change_percent_30m DECIMAL(18,8),
    #     open_interest_change_percent_1h DECIMAL(18,8),
    #     open_interest_change_percent_4h DECIMAL(18,8),
    #     open_interest_change_percent_24h DECIMAL(18,8),
    #     volume_change_percent_5m DECIMAL(18,8),
    #     volume_change_percent_15m DECIMAL(18,8),
    #     volume_change_percent_30m DECIMAL(18,8),
    #     volume_change_percent_1h DECIMAL(18,8),
    #     volume_change_percent_4h DECIMAL(18,8),
    #     volume_change_percent_24h DECIMAL(18,8),
    #     volume_change_usd_1h DECIMAL(38,8),
    #     volume_change_usd_4h DECIMAL(38,8),
    #     volume_change_usd_24h DECIMAL(38,8),
    #     long_short_ratio_5m DECIMAL(18,8),
    #     long_short_ratio_15m DECIMAL(18,8),
    #     long_short_ratio_30m DECIMAL(18,8),
    #     long_short_ratio_1h DECIMAL(18,8),
    #     long_short_ratio_4h DECIMAL(18,8),
    #     long_short_ratio_12h DECIMAL(18,8),
    #     long_short_ratio_24h DECIMAL(18,8),
    #     liquidation_usd_1h DECIMAL(38,8),
    #     long_liquidation_usd_1h DECIMAL(38,8),
    #     short_liquidation_usd_1h DECIMAL(38,8),
    #     liquidation_usd_4h DECIMAL(38,8),
    #     long_liquidation_usd_4h DECIMAL(38,8),
    #     short_liquidation_usd_4h DECIMAL(38,8),
    #     liquidation_usd_12h DECIMAL(38,8),
    #     long_liquidation_usd_12h DECIMAL(38,8),
    #     short_liquidation_usd_12h DECIMAL(38,8),
    #     liquidation_usd_24h DECIMAL(38,8),
    #     long_liquidation_usd_24h DECIMAL(38,8),
    #     short_liquidation_usd_24h DECIMAL(38,8),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_symbol (symbol),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_current_price (current_price),
    #     INDEX idx_market_cap_usd (market_cap_usd),
    #     INDEX idx_open_interest_usd (open_interest_usd),
    #     INDEX idx_long_short_ratio_24h (long_short_ratio_24h)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # ----- Open Interest Tables (DISABLED) -----
    # "cg_open_interest_history": """
    # CREATE TABLE IF NOT EXISTS cg_open_interest_history (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     exchange VARCHAR(50) NOT NULL,
    #     pair VARCHAR(50) NOT NULL,
    #     `interval` VARCHAR(10) NOT NULL,
    #     time BIGINT NOT NULL,
    #     open DECIMAL(38,8),
    #     high DECIMAL(38,8),
    #     low DECIMAL(38,8),
    #     close DECIMAL(38,8),
    #     unit VARCHAR(10),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
    #     INDEX idx_exchange (exchange),
    #     INDEX idx_pair (pair),
    #     INDEX idx_time (time)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # ----- On-chain Tables (DISABLED) -----
    # "cg_exchange_assets": """
    # CREATE TABLE IF NOT EXISTS cg_exchange_assets (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     wallet_address VARCHAR(100) NOT NULL,
    #     balance DECIMAL(38,8),
    #     balance_usd DECIMAL(38,8),
    #     symbol VARCHAR(20) NOT NULL,
    #     assets_name VARCHAR(100),
    #     price DECIMAL(30,10),
    #     exchange VARCHAR(50) NOT NULL,
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_exchange_wallet_address (exchange, wallet_address),
    #     INDEX idx_exchange (exchange),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_wallet_address (wallet_address),
    #     INDEX idx_balance_usd (balance_usd)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # "cg_exchange_balance_list": """
    # CREATE TABLE IF NOT EXISTS cg_exchange_balance_list (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     exchange_name VARCHAR(50) NOT NULL,
    #     total_balance DECIMAL(38,8),
    #     balance_change_1d DECIMAL(38,8),
    #     balance_change_percent_1d DECIMAL(18,8),
    #     balance_change_7d DECIMAL(38,8),
    #     balance_change_percent_7d DECIMAL(18,8),
    #     balance_change_30d DECIMAL(38,8),
    #     balance_change_percent_30d DECIMAL(18,8),
    #     symbol VARCHAR(20) NOT NULL,
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_exchange_name_symbol (exchange_name, symbol),
    #     INDEX idx_exchange_name (exchange_name),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_total_balance (total_balance)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # "cg_exchange_onchain_transfers": """
    # CREATE TABLE IF NOT EXISTS cg_exchange_onchain_transfers (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     transaction_hash VARCHAR(100) NOT NULL,
    #     asset_symbol VARCHAR(20) NOT NULL,
    #     amount_usd DECIMAL(38,8),
    #     asset_quantity DECIMAL(38,8),
    #     exchange_name VARCHAR(50) NOT NULL,
    #     transfer_type TINYINT,
    #     from_address VARCHAR(100),
    #     to_address VARCHAR(100),
    #     transaction_time BIGINT,
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_transaction_hash (transaction_hash),
    #     INDEX idx_exchange_name (exchange_name),
    #     INDEX idx_asset_symbol (asset_symbol),
    #     INDEX idx_transfer_type (transfer_type),
    #     INDEX idx_transaction_time (transaction_time),
    #     INDEX idx_amount_usd (amount_usd),
    #     INDEX idx_from_address (from_address),
    #     INDEX idx_to_address (to_address)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # ----- Spot Orderbook Tables -----
    "cg_spot_orderbook_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_orderbook_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        pair VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        range_percent VARCHAR(10),
        time BIGINT NOT NULL,
        bids_usd DECIMAL(38,8),
        bids_quantity DECIMAL(38,8),
        asks_usd DECIMAL(38,8),
        asks_quantity DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_pair_interval_range_time (exchange, pair, `interval`, range_percent, time),
        INDEX idx_exchange (exchange),
        INDEX idx_pair (pair),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time),
        INDEX idx_range_percent (range_percent)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "cg_spot_orderbook_aggregated": """
    CREATE TABLE IF NOT EXISTS cg_spot_orderbook_aggregated (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange_list VARCHAR(200),
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        range_percent VARCHAR(10),
        time BIGINT NOT NULL,
        aggregated_bids_usd DECIMAL(38,8),
        aggregated_bids_quantity DECIMAL(38,8),
        aggregated_asks_usd DECIMAL(38,8),
        aggregated_asks_quantity DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_list_symbol_interval_range_time (exchange_list, symbol, `interval`, range_percent, time),
        INDEX idx_exchange_list (exchange_list),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time),
        INDEX idx_range_percent (range_percent)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # ----- Spot Market Tables -----
    "cg_spot_coins_markets": """
    CREATE TABLE IF NOT EXISTS cg_spot_coins_markets (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        current_price DECIMAL(30,10),
        market_cap DECIMAL(38,8),
        price_change_5m DECIMAL(30,10),
        price_change_15m DECIMAL(30,10),
        price_change_30m DECIMAL(30,10),
        price_change_1h DECIMAL(30,10),
        price_change_4h DECIMAL(30,10),
        price_change_12h DECIMAL(30,10),
        price_change_24h DECIMAL(30,10),
        price_change_1w DECIMAL(30,10),
        price_change_percent_5m DECIMAL(18,8),
        price_change_percent_15m DECIMAL(18,8),
        price_change_percent_30m DECIMAL(18,8),
        price_change_percent_1h DECIMAL(18,8),
        price_change_percent_4h DECIMAL(18,8),
        price_change_percent_12h DECIMAL(18,8),
        price_change_percent_24h DECIMAL(18,8),
        price_change_percent_1w DECIMAL(18,8),
        volume_usd_1h DECIMAL(38,8),
        volume_usd_5m DECIMAL(38,8),
        volume_usd_15m DECIMAL(38,8),
        volume_usd_30m DECIMAL(38,8),
        volume_usd_4h DECIMAL(38,8),
        volume_usd_12h DECIMAL(38,8),
        volume_usd_24h DECIMAL(38,8),
        volume_usd_1w DECIMAL(38,8),
        volume_change_usd_1h DECIMAL(38,8),
        volume_change_usd_5m DECIMAL(38,8),
        volume_change_usd_15m DECIMAL(38,8),
        volume_change_usd_30m DECIMAL(38,8),
        volume_change_usd_4h DECIMAL(38,8),
        volume_change_usd_12h DECIMAL(38,8),
        volume_change_usd_24h DECIMAL(38,8),
        volume_change_usd_1w DECIMAL(38,8),
        volume_change_percent_1h DECIMAL(18,8),
        volume_change_percent_5m DECIMAL(18,8),
        volume_change_percent_15m DECIMAL(18,8),
        volume_change_percent_30m DECIMAL(18,8),
        volume_change_percent_4h DECIMAL(18,8),
        volume_change_percent_12h DECIMAL(18,8),
        volume_change_percent_24h DECIMAL(18,8),
        volume_change_percent_1w DECIMAL(18,8),
        buy_volume_usd_1h DECIMAL(38,8),
        buy_volume_usd_5m DECIMAL(38,8),
        buy_volume_usd_15m DECIMAL(38,8),
        buy_volume_usd_30m DECIMAL(38,8),
        buy_volume_usd_4h DECIMAL(38,8),
        buy_volume_usd_12h DECIMAL(38,8),
        buy_volume_usd_24h DECIMAL(38,8),
        buy_volume_usd_1w DECIMAL(38,8),
        sell_volume_usd_1h DECIMAL(38,8),
        sell_volume_usd_5m DECIMAL(38,8),
        sell_volume_usd_15m DECIMAL(38,8),
        sell_volume_usd_30m DECIMAL(38,8),
        sell_volume_usd_4h DECIMAL(38,8),
        sell_volume_usd_12h DECIMAL(38,8),
        sell_volume_usd_24h DECIMAL(38,8),
        sell_volume_usd_1w DECIMAL(38,8),
        volume_flow_usd_1h DECIMAL(38,8),
        volume_flow_usd_5m DECIMAL(38,8),
        volume_flow_usd_15m DECIMAL(38,8),
        volume_flow_usd_30m DECIMAL(38,8),
        volume_flow_usd_4h DECIMAL(38,8),
        volume_flow_usd_12h DECIMAL(38,8),
        volume_flow_usd_24h DECIMAL(38,8),
        volume_flow_usd_1w DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol (symbol),
        INDEX idx_symbol (symbol),
        INDEX idx_current_price (current_price),
        INDEX idx_market_cap (market_cap),
        INDEX idx_volume_usd_24h (volume_usd_24h)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "cg_spot_pairs_markets": """
    CREATE TABLE IF NOT EXISTS cg_spot_pairs_markets (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL,
        exchange_name VARCHAR(50) NOT NULL,
        current_price DECIMAL(30,10),
        price_change_1h DECIMAL(30,10),
        price_change_percent_1h DECIMAL(18,8),
        volume_usd_1h DECIMAL(38,8),
        buy_volume_usd_1h DECIMAL(38,8),
        sell_volume_usd_1h DECIMAL(38,8),
        volume_change_usd_1h DECIMAL(38,8),
        volume_change_percent_1h DECIMAL(18,8),
        net_flows_usd_1h DECIMAL(38,8),
        price_change_4h DECIMAL(30,10),
        price_change_percent_4h DECIMAL(18,8),
        volume_usd_4h DECIMAL(38,8),
        buy_volume_usd_4h DECIMAL(38,8),
        sell_volume_usd_4h DECIMAL(38,8),
        volume_change_4h DECIMAL(38,8),
        volume_change_percent_4h DECIMAL(18,8),
        net_flows_usd_4h DECIMAL(38,8),
        price_change_12h DECIMAL(30,10),
        price_change_percent_12h DECIMAL(18,8),
        volume_usd_12h DECIMAL(38,8),
        buy_volume_usd_12h DECIMAL(38,8),
        sell_volume_usd_12h DECIMAL(38,8),
        volume_change_12h DECIMAL(38,8),
        volume_change_percent_12h DECIMAL(18,8),
        net_flows_usd_12h DECIMAL(38,8),
        price_change_24h DECIMAL(30,10),
        price_change_percent_24h DECIMAL(18,8),
        volume_usd_24h DECIMAL(38,8),
        buy_volume_usd_24h DECIMAL(38,8),
        sell_volume_usd_24h DECIMAL(38,8),
        volume_change_24h DECIMAL(38,8),
        volume_change_percent_24h DECIMAL(18,8),
        net_flows_usd_24h DECIMAL(38,8),
        price_change_1w DECIMAL(30,10),
        price_change_percent_1w DECIMAL(18,8),
        volume_usd_1w DECIMAL(38,8),
        buy_volume_usd_1w DECIMAL(38,8),
        sell_volume_usd_1w DECIMAL(38,8),
        volume_change_usd_1w DECIMAL(38,8),
        volume_change_percent_1w DECIMAL(18,8),
        net_flows_usd_1w DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_exchange (symbol, exchange_name),
        INDEX idx_symbol (symbol),
        INDEX idx_exchange_name (exchange_name),
        INDEX idx_current_price (current_price),
        INDEX idx_volume_usd_24h (volume_usd_24h),
        INDEX idx_time_intervals (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Spot Price History Table -----
    "cg_spot_price_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_price_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open DECIMAL(30,10),
        high DECIMAL(30,10),
        low DECIMAL(30,10),
        close DECIMAL(30,10),
        volume_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_symbol_interval_time (exchange, symbol, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time),
        INDEX idx_volume_usd (volume_usd)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Bitcoin ETF Tables (DISABLED) -----
    # "cg_bitcoin_etf_history": """
    # CREATE TABLE IF NOT EXISTS cg_bitcoin_etf_history (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     ticker VARCHAR(20) NOT NULL,
    #     name VARCHAR(200),
    #     assets_date BIGINT,
    #     btc_holdings DECIMAL(38,8),
    #     market_date BIGINT,
    #     market_price DECIMAL(30,10),
    #     nav DECIMAL(30,10),
    #     net_assets DECIMAL(38,8),
    #     premium_discount DECIMAL(18,8),
    #     shares_outstanding BIGINT,
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_ticker_assets_date (ticker, assets_date),
    #     INDEX idx_ticker (ticker),
    #     INDEX idx_assets_date (assets_date),
    #     INDEX idx_market_date (market_date),
    #     INDEX idx_premium_discount (premium_discount),
    #     INDEX idx_net_assets (net_assets)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # ===== ACTIVE TABLES BELOW =====

    # ----- Long/Short Ratio Tables -----
    "cg_long_short_global_account_ratio_history": """
    CREATE TABLE IF NOT EXISTS cg_long_short_global_account_ratio_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        pair VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        global_account_long_percent DECIMAL(18,8),
        global_account_short_percent DECIMAL(18,8),
        global_account_long_short_ratio DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_pair (pair),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_long_short_top_account_ratio_history": """
    CREATE TABLE IF NOT EXISTS cg_long_short_top_account_ratio_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        pair VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        top_account_long_percent DECIMAL(18,8),
        top_account_short_percent DECIMAL(18,8),
        top_account_long_short_ratio DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_pair (pair),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='Top Account Long/Short Ratio History - Endpoint: /api/futures/top-long-short-account-ratio/history'
    """,

    # ----- Liquidation Tables -----
    "cg_liquidation_aggregated_history": """
    CREATE TABLE IF NOT EXISTS cg_liquidation_aggregated_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        aggregated_long_liquidation_usd DECIMAL(38,8),
        aggregated_short_liquidation_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_interval_time (symbol, `interval`, time),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Liquidation Heatmap Tables (Relational Structure) -----
    "cg_liquidation_heatmap": """
    CREATE TABLE IF NOT EXISTS cg_liquidation_heatmap (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        `range` VARCHAR(10) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_range (symbol, `range`),
        INDEX idx_symbol (symbol),
        INDEX idx_range (`range`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_liquidation_heatmap_y_axis": """
    CREATE TABLE IF NOT EXISTS cg_liquidation_heatmap_y_axis (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        liquidation_heatmap_id BIGINT NOT NULL,
        price_level DECIMAL(18,8) NOT NULL,
        sequence_order INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_liquidation_heatmap_id (liquidation_heatmap_id),
        INDEX idx_sequence_order (sequence_order),
        FOREIGN KEY (liquidation_heatmap_id) REFERENCES cg_liquidation_heatmap(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_liquidation_heatmap_leverage_data": """
    CREATE TABLE IF NOT EXISTS cg_liquidation_heatmap_leverage_data (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        liquidation_heatmap_id BIGINT NOT NULL,
        sequence_order INT NOT NULL,
        x_position INT,
        y_position INT,
        liquidation_amount DECIMAL(20,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_liquidation_heatmap_id (liquidation_heatmap_id),
        INDEX idx_sequence_order (sequence_order),
        FOREIGN KEY (liquidation_heatmap_id) REFERENCES cg_liquidation_heatmap(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_liquidation_heatmap_price_candlesticks": """
    CREATE TABLE IF NOT EXISTS cg_liquidation_heatmap_price_candlesticks (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        liquidation_heatmap_id BIGINT NOT NULL,
        sequence_order INT NOT NULL,
        timestamp BIGINT NOT NULL,
        open_price DECIMAL(20,8),
        high_price DECIMAL(20,8),
        low_price DECIMAL(20,8),
        close_price DECIMAL(20,8),
        volume DECIMAL(20,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_liquidation_heatmap_id (liquidation_heatmap_id),
        INDEX idx_sequence_order (sequence_order),
        INDEX idx_timestamp (timestamp),
        FOREIGN KEY (liquidation_heatmap_id) REFERENCES cg_liquidation_heatmap(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Futures Basis Tables -----
    "cg_futures_basis_history": """
    CREATE TABLE IF NOT EXISTS cg_futures_basis_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        pair VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open_basis DECIMAL(18,8),
        close_basis DECIMAL(18,8),
        open_change DECIMAL(18,8),
        close_change DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_pair (pair),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # "cg_long_short_position_ratio_history": """  # DISABLED - Position endpoint commented out
    # CREATE TABLE IF NOT EXISTS cg_long_short_position_ratio_history (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     exchange VARCHAR(50) NOT NULL,
    #     pair VARCHAR(50) NOT NULL,
    #     `interval` VARCHAR(10) NOT NULL,
    #     time BIGINT NOT NULL,
    #     top_position_long_percent DECIMAL(18,8),
    #     top_position_short_percent DECIMAL(18,8),
    #     top_position_long_short_ratio DECIMAL(18,8),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_exchange_pair_interval_time (exchange, pair, `interval`, time),
    #     INDEX idx_exchange (exchange),
    #     INDEX idx_pair (pair),
    #     INDEX idx_time (time)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # ----- Options Tables (DISABLED) -----
    # "cg_option_max_pain": """
    # CREATE TABLE IF NOT EXISTS cg_option_max_pain (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     symbol VARCHAR(20) NOT NULL,
    #     exchange VARCHAR(50) NOT NULL,
    #     date VARCHAR(10) NOT NULL,
    #     call_open_interest_market_value DECIMAL(38,8),
    #     put_open_interest DECIMAL(38,8),
    #     put_open_interest_market_value DECIMAL(38,8),
    #     max_pain_price DECIMAL(30,10),
    #     call_open_interest DECIMAL(38,8),
    #     call_open_interest_notational DECIMAL(38,8),
    #     put_open_interest_notational DECIMAL(38,8),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_symbol_exchange_date (symbol, exchange, date),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_exchange (exchange),
    #     INDEX idx_date (date)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    # "cg_option_info": """
    # CREATE TABLE IF NOT EXISTS cg_option_info (
    #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
    #     symbol VARCHAR(20) NOT NULL,
    #     exchange VARCHAR(50) NOT NULL,
    #     open_interest DECIMAL(38,8),
    #     oi_market_share DECIMAL(18,8),
    #     open_interest_change_24h DECIMAL(18,8),
    #     open_interest_usd DECIMAL(38,8),
    #     volume_usd_24h DECIMAL(38,8),
    #     volume_change_percent_24h DECIMAL(18,8),
    #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #     UNIQUE KEY uk_symbol_exchange (symbol, exchange),
    #     INDEX idx_symbol (symbol),
    #     INDEX idx_exchange (exchange)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # """,

    
    
    
    "cg_spot_price_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_price_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open DECIMAL(30,10),
        high DECIMAL(30,10),
        low DECIMAL(30,10),
        close DECIMAL(30,10),
        volume_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_symbol_interval_time (exchange, symbol, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time),
        INDEX idx_symbol_time (symbol, time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # ----- Bitcoin ETF Tables -----
    "cg_bitcoin_etf_list": """
    CREATE TABLE IF NOT EXISTS cg_bitcoin_etf_list (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(20) NOT NULL,
        fund_name VARCHAR(200) NOT NULL,
        region VARCHAR(10),
        market_status VARCHAR(50),
        primary_exchange VARCHAR(20),
        cik_code VARCHAR(20),
        fund_type VARCHAR(20),
        market_cap_usd DECIMAL(38,8),
        list_date BIGINT,
        shares_outstanding VARCHAR(50),
        aum_usd DECIMAL(38,8),
        management_fee_percent VARCHAR(20),
        last_trade_time BIGINT,
        last_quote_time BIGINT,
        volume_quantity BIGINT,
        volume_usd DECIMAL(38,8),
        price_usd DECIMAL(30,10),
        price_change_usd DECIMAL(30,10),
        price_change_percent DECIMAL(18,8),
        btc_holding DECIMAL(38,8),
        net_asset_value_usd DECIMAL(30,10),
        premium_discount_percent DECIMAL(18,8),
        btc_change_percent_24h DECIMAL(18,8),
        btc_change_24h DECIMAL(30,10),
        btc_change_percent_7d DECIMAL(18,8),
        btc_change_7d DECIMAL(30,10),
        update_date VARCHAR(20),
        update_timestamp BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_ticker (ticker),
        INDEX idx_ticker (ticker),
        INDEX idx_fund_name (fund_name),
        INDEX idx_region (region),
        INDEX idx_market_cap_usd (market_cap_usd),
        INDEX idx_aum_usd (aum_usd),
        INDEX idx_update_timestamp (update_timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    
    
    "cg_bitcoin_etf_premium_discount_history": """
    CREATE TABLE IF NOT EXISTS cg_bitcoin_etf_premium_discount_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        timestamp BIGINT NOT NULL,
        ticker VARCHAR(20),  -- Nullable to support aggregated data
        nav_usd DECIMAL(30,10),
        market_price_usd DECIMAL(30,10),
        premium_discount_details DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_timestamp_ticker (timestamp, ticker),
        INDEX idx_timestamp (timestamp),
        INDEX idx_ticker (ticker),
        INDEX idx_premium_discount_details (premium_discount_details),
        INDEX idx_nav_usd (nav_usd),
        INDEX idx_market_price_usd (market_price_usd)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_bitcoin_etf_flows_history": """
    CREATE TABLE IF NOT EXISTS cg_bitcoin_etf_flows_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        timestamp BIGINT NOT NULL UNIQUE,
        flow_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_timestamp (timestamp),
        INDEX idx_flow_usd (flow_usd)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_bitcoin_etf_flows_details": """
    CREATE TABLE IF NOT EXISTS cg_bitcoin_etf_flows_details (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        timestamp BIGINT NOT NULL,
        etf_ticker VARCHAR(20) NOT NULL,
        flow_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_timestamp_ticker (timestamp, etf_ticker),
        INDEX idx_timestamp (timestamp),
        INDEX idx_etf_ticker (etf_ticker),
        INDEX idx_flow_usd (flow_usd)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

  
    # ----- Macro Overlay Tables -----
    "cg_bitcoin_vs_global_m2_growth": """
    CREATE TABLE IF NOT EXISTS cg_bitcoin_vs_global_m2_growth (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        timestamp BIGINT NOT NULL UNIQUE,
        price DECIMAL(30,10),
        global_m2_yoy_growth DECIMAL(18,8),
        global_m2_supply DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_timestamp (timestamp),
        INDEX idx_price (price),
        INDEX idx_global_m2_supply (global_m2_supply)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Options Tables -----
    "cg_option_exchange_oi_history": """
    CREATE TABLE IF NOT EXISTS cg_option_exchange_oi_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        unit VARCHAR(10) NOT NULL,
        `range` VARCHAR(10) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_unit_range (symbol, unit, `range`),
        INDEX idx_symbol (symbol),
        INDEX idx_unit (unit),
        INDEX idx_range (`range`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_option_exchange_oi_history_time_list": """
    CREATE TABLE IF NOT EXISTS cg_option_exchange_oi_history_time_list (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        option_exchange_oi_history_id BIGINT NOT NULL,
        time_value BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (option_exchange_oi_history_id) REFERENCES cg_option_exchange_oi_history(id) ON DELETE CASCADE,
        INDEX idx_option_exchange_oi_history_id (option_exchange_oi_history_id),
        INDEX idx_time_value (time_value),
        UNIQUE KEY uk_oi_history_time (option_exchange_oi_history_id, time_value)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_option_exchange_oi_history_exchange_data": """
    CREATE TABLE IF NOT EXISTS cg_option_exchange_oi_history_exchange_data (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        option_exchange_oi_history_id BIGINT NOT NULL,
        exchange_name VARCHAR(50) NOT NULL,
        oi_value DECIMAL(20,8) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (option_exchange_oi_history_id) REFERENCES cg_option_exchange_oi_history(id) ON DELETE CASCADE,
        INDEX idx_option_exchange_oi_history_id (option_exchange_oi_history_id),
        INDEX idx_exchange_name (exchange_name),
        UNIQUE KEY uk_oi_history_exchange (option_exchange_oi_history_id, exchange_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Sentiment Tables -----
    "cg_fear_greed_index": """
    CREATE TABLE IF NOT EXISTS cg_fear_greed_index (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        fetch_timestamp BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_fetch_timestamp (fetch_timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_fear_greed_index_data_list": """
    CREATE TABLE IF NOT EXISTS cg_fear_greed_index_data_list (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        fear_greed_index_id BIGINT NOT NULL,
        index_value INT NOT NULL,
        sequence_order INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (fear_greed_index_id) REFERENCES cg_fear_greed_index(id) ON DELETE CASCADE,
        INDEX idx_fear_greed_index_id (fear_greed_index_id),
        INDEX idx_sequence_order (sequence_order)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_hyperliquid_whale_alert": """
    CREATE TABLE IF NOT EXISTS cg_hyperliquid_whale_alert (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user VARCHAR(100) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        position_size DECIMAL(38,8),
        entry_price DECIMAL(30,10),
        liq_price DECIMAL(30,10),
        position_value_usd DECIMAL(38,8),
        position_action TINYINT,
        create_time BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_user_symbol_create_time (user, symbol, create_time),
        INDEX idx_user (user),
        INDEX idx_symbol (symbol),
        INDEX idx_create_time (create_time),
        INDEX idx_position_value_usd (position_value_usd)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_whale_transfer": """
    CREATE TABLE IF NOT EXISTS cg_whale_transfer (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        transaction_hash VARCHAR(100) NOT NULL UNIQUE,
        amount_usd DECIMAL(38,8),
        asset_quantity DECIMAL(38,8),
        asset_symbol VARCHAR(20) NOT NULL,
        from_address VARCHAR(100),
        to_address VARCHAR(100),
        blockchain_name VARCHAR(50),
        block_height BIGINT,
        block_timestamp BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_transaction_hash (transaction_hash),
        INDEX idx_asset_symbol (asset_symbol),
        INDEX idx_blockchain_name (blockchain_name),
        INDEX idx_block_timestamp (block_timestamp),
        INDEX idx_amount_usd (amount_usd),
        INDEX idx_from_address (from_address),
        INDEX idx_to_address (to_address)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Open Interest Aggregated Stablecoin History Table -----
    "cg_open_interest_aggregated_stablecoin_history": """
    CREATE TABLE IF NOT EXISTS cg_open_interest_aggregated_stablecoin_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange_list VARCHAR(200) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        open DECIMAL(38,8),
        high DECIMAL(38,8),
        low DECIMAL(38,8),
        close DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_list_symbol_interval_time (exchange_list, symbol, `interval`, time),
        INDEX idx_exchange_list (exchange_list),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ===== NEW ENDPOINTS TABLES =====

    "cg_futures_footprint_history": """
    CREATE TABLE IF NOT EXISTS cg_futures_footprint_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        time BIGINT NOT NULL,
        price_start DECIMAL(20,8),
        price_end DECIMAL(20,8),
        taker_buy_volume DECIMAL(38,8),
        taker_sell_volume DECIMAL(38,8),
        taker_buy_volume_usd DECIMAL(38,8),
        taker_sell_volume_usd DECIMAL(38,8),
        taker_buy_trades INT,
        taker_sell_trades INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_symbol_interval_time_price (exchange, symbol, `interval`, time, price_start, price_end),
        INDEX idx_exchange (exchange),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_spot_large_orderbook_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_large_orderbook_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        order_id BIGINT NOT NULL,
        exchange_name VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        base_asset VARCHAR(20) NOT NULL,
        quote_asset VARCHAR(20) NOT NULL,
        price DECIMAL(20,8),
        start_time BIGINT NOT NULL,
        start_quantity DECIMAL(38,8),
        start_usd_value DECIMAL(38,8),
        current_quantity DECIMAL(38,8),
        current_usd_value DECIMAL(38,8),
        `current_time` BIGINT NOT NULL,
        executed_volume DECIMAL(38,8),
        executed_usd_value DECIMAL(38,8),
        trade_count INT,
        order_side TINYINT,
        order_state TINYINT,
        order_end_time BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_order_id (order_id),
        INDEX idx_exchange_name (exchange_name),
        INDEX idx_symbol (symbol),
        INDEX idx_base_asset (base_asset),
        INDEX idx_start_time (start_time),
        INDEX idx_order_state (order_state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_spot_large_orderbook": """
    CREATE TABLE IF NOT EXISTS cg_spot_large_orderbook (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        order_id BIGINT NOT NULL,
        exchange_name VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        base_asset VARCHAR(20) NOT NULL,
        quote_asset VARCHAR(20) NOT NULL,
        price DECIMAL(20,8),
        start_time BIGINT NOT NULL,
        start_quantity DECIMAL(38,8),
        start_usd_value DECIMAL(38,8),
        current_quantity DECIMAL(38,8),
        current_usd_value DECIMAL(38,8),
        `current_time` BIGINT NOT NULL,
        executed_volume DECIMAL(38,8),
        executed_usd_value DECIMAL(38,8),
        trade_count INT,
        order_side TINYINT,
        order_state TINYINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_order_id (order_id),
        INDEX idx_exchange_name (exchange_name),
        INDEX idx_symbol (symbol),
        INDEX idx_base_asset (base_asset),
        INDEX idx_current_time (`current_time`),
        INDEX idx_order_state (order_state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_spot_aggregated_taker_volume_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_aggregated_taker_volume_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange_list VARCHAR(200) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        unit VARCHAR(10) NOT NULL DEFAULT 'usd',
        time BIGINT NOT NULL,
        aggregated_buy_volume_usd DECIMAL(38,8),
        aggregated_sell_volume_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_list_symbol_interval_time (exchange_list, symbol, `interval`, time),
        INDEX idx_exchange_list (exchange_list),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "cg_spot_taker_volume_history": """
    CREATE TABLE IF NOT EXISTS cg_spot_taker_volume_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        unit VARCHAR(10) NOT NULL DEFAULT 'usd',
        time BIGINT NOT NULL,
        aggregated_buy_volume_usd DECIMAL(38,8),
        aggregated_sell_volume_usd DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_symbol_interval_time (exchange, symbol, `interval`, time),
        INDEX idx_exchange (exchange),
        INDEX idx_symbol (symbol),
        INDEX idx_interval (`interval`),
        INDEX idx_time (time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}
