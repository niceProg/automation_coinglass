# app/database/connection.py
import pymysql
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


def get_connection():
    """Get database connection."""
    try:
        conn = pymysql.connect(
            host=settings.DB["HOST"],
            port=settings.DB["PORT"],
            user=settings.DB["USER"],
            password=settings.DB["PASSWORD"],
            database=settings.DB["NAME"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        logger.info("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None
