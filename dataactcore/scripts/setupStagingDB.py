from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB


def setupStagingDB():
    """Create job tracker tables from model metadata."""
    createDatabase(CONFIG_DB['staging_db_name'])
    runMigrations('staging')

if __name__ == '__main__':
    setupStagingDB()