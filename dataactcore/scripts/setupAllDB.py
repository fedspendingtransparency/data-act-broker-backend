from dataactcore.config import CONFIG_DB
from dataactcore.logging import configure_logging
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupJobQueueDB import setupJobQueueDB
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupValidationDB import setupValidationDB


def setupAllDB():
    """Sets up all databases"""
    createDatabase(CONFIG_DB['db_name'])
    runMigrations()
    setupJobTrackerDB()
    setupErrorDB()
    setupUserDB()
    setupJobQueueDB()
    setupValidationDB()


if __name__ == '__main__':
    configure_logging()
    setupAllDB()
