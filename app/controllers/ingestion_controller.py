# app/controllers/ingestion_controller.py
import logging
from typing import List, Optional
from app.services.coinglass_service import CoinglassService

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

    def setup_database(self):
        """Setup database tables."""
        try:
            service = CoinglassService(ensure_tables=True)
            self.logger.info("Database setup completed successfully")
            return {"status": "success", "message": "Database tables created"}
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}
        finally:
            if "service" in locals():
                service.close()
