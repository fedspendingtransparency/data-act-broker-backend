import logging

# we want to allow access to, e.g. config.CONFIG_BROKER
from dataactcore.read_config import (   # noqa
    CONFIG_BROKER, CONFIG_LOGGING, CONFIG_SERVICES, CONFIG_DB,
    CONFIG_JOB_QUEUE, CONFIG_PATH, ALEMBIC_PATH, MIGRATION_PATH, log_message)

# Log config values along with warnings for missing files
if log_message:
    logging.getLogger('deprecated.info').info(log_message)
