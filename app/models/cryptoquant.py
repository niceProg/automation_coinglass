# app/models/cryptoquant.py

CRYPTOQUANT_TABLES = {
    # ----- Exchange Inflow CDD Tables -----
    "cq_exchange_inflow_cdd": """
    CREATE TABLE IF NOT EXISTS cq_exchange_inflow_cdd (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        exchange VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        `interval` VARCHAR(10) NOT NULL,
        value DECIMAL(20,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_exchange_date_interval (exchange, date, `interval`),
        INDEX idx_exchange (exchange),
        INDEX idx_date (date),
        INDEX idx_interval (`interval`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ----- Bitcoin Market Price Tables -----
    "cq_btc_market_price": """
    CREATE TABLE IF NOT EXISTS cq_btc_market_price (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        date DATE NOT NULL,
        open DECIMAL(18,8),
        high DECIMAL(18,8),
        low DECIMAL(18,8),
        close DECIMAL(18,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_date (date),
        INDEX idx_date (date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}