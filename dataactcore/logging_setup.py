import logging

from dataactcore.config import CONFIG_LOGGING


logger = logging.getLogger(__name__)


def configure_logging():
    """Read settings, create a logger from them. Nothing too fancy"""
    log_level = CONFIG_LOGGING.get('loglevel', 'INFO') or ''
    log_level = log_level.upper()
    configured_correctly = hasattr(logging, log_level)

    if not configured_correctly:
        log_level = 'INFO'

    logging.basicConfig(level=log_level)

    # Must configure logging before we can log the logging config error...
    if not configured_correctly:
        logger.warning("Invalid loglevel: %s", CONFIG_LOGGING['loglevel'])
