import logging
from src.application.services.orchestration import WeatherPipelineOrchestrator
from src.shared.config.settings import settings
from src.shared.logging.logger import get_logger

logger = get_logger(__name__)


def main():
    logger.info("Starting Weather Data Pipeline")
    logger.info(f"Processing {len(settings.CITIES)} cities")
    
    try:
        # Initialize orchestrator
        orchestrator = WeatherPipelineOrchestrator()
        
        # Run the full pipeline
        success = orchestrator.run_full_pipeline(settings.CITIES)
        
        if success:
            logger.info("Pipeline completed successfully!")
        else:
            logger.error("Pipeline failed!")
            
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise
    finally:
        # Cleanup resources
        orchestrator.cleanup()


if __name__ == '__main__':
    main()
