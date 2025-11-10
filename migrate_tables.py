#!/usr/bin/env python3
"""
Migration Script for Coinglass Database Tables

This script handles migrations for:
1. fear_greed_index: Change from JSON structure to parent-child relational structure
2. long_short_ratio_top: Rename table from cg_long_short_account_ratio_history to cg_long_short_top_account_ratio_history

Usage:
    python migrate_tables.py
"""

import pymysql
from app.database.connection import get_connection
from app.core.logging import setup_logger

logger = setup_logger(__name__)


def migrate_fear_greed_index(conn):
    """
    Migrate fear_greed_index from JSON structure to relational structure.

    Old structure: cg_fear_greed_index with data_list JSON column
    New structure:
        - cg_fear_greed_index with fetch_timestamp
        - cg_fear_greed_index_data_list (child table with foreign key)
    """
    try:
        with conn.cursor() as cur:
            # Check if old table exists
            cur.execute("SHOW TABLES LIKE 'cg_fear_greed_index'")
            if not cur.fetchone():
                logger.info("✓ fear_greed_index table doesn't exist, will be created fresh")
                return

            # Check if table has old structure (data_list column)
            cur.execute("SHOW COLUMNS FROM cg_fear_greed_index LIKE 'data_list'")
            has_old_structure = cur.fetchone() is not None

            if has_old_structure:
                logger.info("🔄 Migrating fear_greed_index from JSON to relational structure...")

                # Drop the old table (we can't migrate the data easily from JSON)
                logger.warning("⚠️  Dropping old fear_greed_index table (data will be lost)")
                cur.execute("DROP TABLE IF EXISTS cg_fear_greed_index")

                logger.info("✓ Old fear_greed_index table dropped successfully")
            else:
                logger.info("✓ fear_greed_index already has new structure")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error migrating fear_greed_index: {e}")
        raise


def migrate_long_short_ratio_top(conn):
    """
    Rename cg_long_short_account_ratio_history to cg_long_short_top_account_ratio_history.
    """
    try:
        with conn.cursor() as cur:
            # Check if old table exists
            cur.execute("SHOW TABLES LIKE 'cg_long_short_account_ratio_history'")
            old_table_exists = cur.fetchone() is not None

            # Check if new table exists
            cur.execute("SHOW TABLES LIKE 'cg_long_short_top_account_ratio_history'")
            new_table_exists = cur.fetchone() is not None

            if old_table_exists and not new_table_exists:
                logger.info("🔄 Renaming cg_long_short_account_ratio_history to cg_long_short_top_account_ratio_history...")

                # Rename the table
                cur.execute("""
                    RENAME TABLE
                        cg_long_short_account_ratio_history
                    TO
                        cg_long_short_top_account_ratio_history
                """)

                logger.info("✓ Table renamed successfully")

            elif new_table_exists and old_table_exists:
                logger.warning("⚠️  Both old and new tables exist. Dropping old table...")
                cur.execute("DROP TABLE cg_long_short_account_ratio_history")
                logger.info("✓ Old table dropped")

            elif new_table_exists:
                logger.info("✓ Table already has new name: cg_long_short_top_account_ratio_history")

            else:
                logger.info("✓ Neither table exists, will be created fresh")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error migrating long_short_ratio_top: {e}")
        raise


def main():
    """Run all migrations."""
    logger.info("=" * 60)
    logger.info("🚀 Starting Database Migration")
    logger.info("=" * 60)

    conn = None
    try:
        conn = get_connection()

        # Run migrations
        logger.info("\n1️⃣  Migrating fear_greed_index...")
        migrate_fear_greed_index(conn)

        logger.info("\n2️⃣  Migrating long_short_ratio_top...")
        migrate_long_short_ratio_top(conn)

        logger.info("\n" + "=" * 60)
        logger.info("✅ All migrations completed successfully!")
        logger.info("=" * 60)
        logger.info("\nNext steps:")
        logger.info("  1. Run: python main.py --setup")
        logger.info("  2. This will create the new table structures")
        logger.info("  3. Then run your pipelines normally")

    except Exception as e:
        logger.error(f"\n❌ Migration failed: {e}")
        return 1
    finally:
        if conn:
            conn.close()

    return 0


if __name__ == "__main__":
    exit(main())
