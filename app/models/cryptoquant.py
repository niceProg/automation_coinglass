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
}