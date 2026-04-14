"""
Centralized logging configuration for the Weather Data Pipeline.
Provides consistent logging across all modules.
"""
import logging
import sys
from typing import Optional
from src.shared.config.settings import settings


class LoggerFactory:
    """
    Factory for creating configured logger instances.
    Ensures consistent logging format and level across the application.
    """

    _configured: bool = False

    @classmethod
    def configure_root_logger(cls) -> None:
        """Configure the root logger with consistent formatting."""
        if cls._configured:
            return

        # Create root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, settings.LOG_LEVEL))

        # Create console handler with formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, settings.LOG_LEVEL))

        # Create formatter
        formatter = logging.Formatter(
            fmt=settings.LOG_FORMAT,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)

        # Add handler to root logger
        if not root_logger.handlers:
            root_logger.addHandler(console_handler)

        cls._configured = True

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        """
        Get a configured logger instance.

        Args:
            name: Logger name (typically __name__)

        Returns:
            Configured logger instance
        """
        cls.configure_root_logger()
        return logging.getLogger(name)


# Create module-level function for easy imports
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a configured logger instance."""
    return LoggerFactory.get_logger(name)
