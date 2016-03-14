from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB

def setupAllDB():
    """Sets up all databases"""
    setupJobTrackerDB(hardReset=True)
    print ("Created job_tracker database")
    print ("Created user database")
    setupErrorDB(True)
    print ("Created error database")
    setupUserDB(True)
    print("Created user database")
    
if __name__ == '__main__':
    setupAllDB()
