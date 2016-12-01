from dataactcore.config import CONFIG_DB
from dataactcore.logging import configure_logging
from dataactcore.scripts.databaseSetup import createDatabase


def setupJobQueueDB():
    """Create job tracker tables from model metadata."""
    createDatabase(CONFIG_DB['job_queue_db_name'])


if __name__ == '__main__':
    configure_logging()
    setupJobQueueDB()
