from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobQueueDB import setupJobQueueDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.setupStagingDB import setupStagingDB

def setupAllDB():
    """Sets up all databases"""
    setupJobTrackerDB()
    setupErrorDB()
    setupUserDB()
    setupJobQueueDB()
    setupValidationDB()
    setupStagingDB()

if __name__ == '__main__':
    setupAllDB()
