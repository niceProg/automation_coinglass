# app/core/config.py
import os
from pathlib import Path
from typing import Any, Dict, List
from dotenv import load_dotenv

# Resolve project root & load .env
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(str(ROOT_DIR / ".env"))


def _env_list(name: str, default: List[str]) -> List[str]:
    """Parse CSV environment variable to list of strings."""
    raw = os.getenv(name)
    if not raw or not raw.strip():
        return list(default)
    return [x.strip() for x in raw.split(",") if x.strip()]


class Settings:
    # ---------- Database ----------
    DB: Dict[str, Any] = {
        "HOST": os.getenv("DB_HOST"),
        "PORT": int(os.getenv("DB_PORT")),
        "USER": os.getenv("DB_USER"),
        "PASSWORD": os.getenv("DB_PASSWORD"),
        "NAME": os.getenv("DB_NAME"),
    }

    @property
    def DB_SQLALCHEMY_URL(self) -> str:
        pwd = self.DB["PASSWORD"]
        user = self.DB["USER"]
        host = self.DB["HOST"]
        port = self.DB["PORT"]
        name = self.DB["NAME"]
        return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"

    # ---------- Coinglass ----------
    COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

    # Default exchanges and symbols
    COINGLASS_EXCHANGES = _env_list(
        "COINGLASS_EXCHANGES",
        [
            "OKX",
            "Binance",
            "HTX",
            "Bitmex",
            "Bitfinex",
            "Bybit",
            "KuCoin",
            "Bitget",
            "CoinEx",
            "BingX",
            "MEXC",
            "Deribit",
            "Gate",
            # "Gemini", "Crypto.com", "Hyperliquid", "Bitunix", , "WhiteBIT", "Aster",
            # "Lighter", "EdgeX", "Drift", "Paradex", "Extended", "ApeX Omni"
        ],
    )
    COINGLASS_SYMBOLS = _env_list("COINGLASS_SYMBOLS", ["BTC"])

    # Minimum USD for filtering
    MIN_USD = float(os.getenv("MIN_USD", "100"))


settings = Settings()
