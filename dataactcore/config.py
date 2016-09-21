from dataactcore.read_config import CONFIG_BROKER, CONFIG_LOGGING, CONFIG_SERVICES, CONFIG_DB, CONFIG_JOB_QUEUE, log_message
from dataactcore.utils.cloudLogger import CloudLogger

# Log config values along with warnings for missing files
CloudLogger.log(log_message)
