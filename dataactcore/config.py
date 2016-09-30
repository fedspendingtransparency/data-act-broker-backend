from dataactcore.read_config import CONFIG_BROKER, CONFIG_LOGGING, CONFIG_SERVICES, CONFIG_DB, CONFIG_JOB_QUEUE, \
     CONFIG_PATH, ALEMBIC_PATH, MIGRATION_PATH, log_message
from dataactcore.utils.cloudLogger import CloudLogger

# Log config values along with warnings for missing files
if log_message:
    CloudLogger.log(log_message)
