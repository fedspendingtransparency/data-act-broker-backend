from dataactcore.scripts.databaseSetup import createDatabase
from dataactcore.config import CONFIG_DB


def setupStagingDB():
    """Create the staging database."""
    createDatabase(CONFIG_DB['staging_db_name'])

if __name__ == '__main__':
    setupStagingDB()
