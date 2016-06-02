from dataactcore.scripts.databaseSetup import createDatabase
from dataactcore.config import CONFIG_DB


def setupJobQueueDB():
    """Create job tracker tables from model metadata."""
    createDatabase(CONFIG_DB['job_queue_db_name'])

if __name__ == '__main__':
    setupJobQueueDB()
