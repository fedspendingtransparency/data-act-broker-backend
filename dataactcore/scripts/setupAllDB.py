from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobQueueDB import setupJobQueueDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB

def setupAllDB():
    """Sets up all databases"""
    createDatabase(CONFIG_DB['db_name'])
    runMigrations(CONFIG_DB['db_name'])
    setupJobTrackerDB()
    setupErrorDB()
    setupUserDB()
    setupJobQueueDB()
    setupValidationDB()

if __name__ == '__main__':
    setupAllDB()
