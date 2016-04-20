from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB

def setupAllDB():
    """Sets up all databases"""
    setupJobTrackerDB(hardReset=True)
    setupErrorDB(True)
    setupUserDB(True)
    
if __name__ == '__main__':
    setupAllDB()
