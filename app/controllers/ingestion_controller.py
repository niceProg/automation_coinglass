# app/controllers/ingestion_controller.py
import logging
from typing import List, Optional
from app.services.coinglass_service import CoinglassService
from app.services.cryptoquant_service import CryptoQuantService

logger = logging.getLogger(__name__)


class IngestionController:
    """Controller to manage data ingestion operations."""

    def __init__(self):
        # Setup logger
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger("app.controller")

    def run_coinglass(self, pipelines: Optional[List[str]] = None):
        """Run Coinglass pipelines."""
        try:
            service = CoinglassService(ensure_tables=False)
            if pipelines:
                return service.run_selected_pipelines(pipelines)
            else:
                return service.run_all_pipelines()
        except Exception as e:
            self.logger.error(f"Coinglass service failed: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if "service" in locals():
                service.close()

    def run_initial_scrape(self, months: int = 1):
        """Run initial historical data scrape."""
        try:
            service = CoinglassService(ensure_tables=False)
            return service.run_initial_scrape(months=months)
        except Exception as e:
            self.logger.error(f"Initial scrape failed: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if "service" in locals():
                service.close()

    def run_pipeline_with_interval(self, pipeline_name: str, interval: str, days: int):
        """Run a specific pipeline with interval-based historical data retrieval."""
        try:
            service = CoinglassService(ensure_tables=False)
            return service.run_pipeline_with_interval(pipeline_name, interval, days)
        except Exception as e:
            self.logger.error(f"Pipeline {pipeline_name} failed: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if "service" in locals():
                service.close()

    def get_status(self):
        """Get ingestion status."""
        try:
            service = CoinglassService(ensure_tables=False)
            return service.get_status()
        except Exception as e:
            self.logger.error(f"Status check failed: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if "service" in locals():
                service.close()

    def check_freshness(self):
        """Check data freshness for all pipelines."""
        try:
            service = CoinglassService(ensure_tables=False)
            return service.check_and_log_freshness()
        except Exception as e:
            self.logger.error(f"Freshness check failed: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if "service" in locals():
                service.close()

    def run_cryptoquant(self, pipelines: Optional[List[str]] = None):
        """Run CryptoQuant pipelines."""
        try:
            service = CryptoQuantService()
            if pipelines:
                results = {}
                for pipeline in pipelines:
                    result = service.run_pipeline(pipeline)
                    results[pipeline] = result
                return results
            else:
                return service.run_all_pipelines()
        except Exception as e:
            self.logger.error(f"CryptoQuant service failed: {e}", exc_info=True)
            return {"error": str(e)}

    def setup_database(self):
        """Setup database tables."""
        try:
            # Setup Coinglass tables
            coinglass_service = CoinglassService(ensure_tables=True)
            self.logger.info("Coinglass database setup completed successfully")
            coinglass_service.close()

            # Setup CryptoQuant tables
            from app.models.cryptoquant import CRYPTOQUANT_TABLES
            from app.database.connection import get_connection

            conn = get_connection()
            cursor = conn.cursor()

            for table_name, create_query in CRYPTOQUANT_TABLES.items():
                self.logger.info(f"Creating table: {table_name}")
                cursor.execute(create_query)

            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info("CryptoQuant database setup completed successfully")
            return {"status": "success", "message": "All database tables created"}
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}
        finally:
            # Clean up any open connections
            try:
                if "coinglass_service" in locals():
                    coinglass_service.close()
            except:
                pass  # Connection already closed, ignore error

            try:
                if "conn" in locals() and hasattr(conn, 'open') and conn.open:
                    conn.close()
            except:
                pass  # Connection already closed, ignore error
