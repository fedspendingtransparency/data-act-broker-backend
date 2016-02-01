from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
import dataactcore.scripts.setupStaging

def setupAllDB():
    """Sets up all databases"""
    setupJobTrackerDB(hardReset=True)
    print ("Created job_tracker database")
    print ("Created user database")
    setupValidationDB(True)
    print ("Created validation database")
    setupErrorDB(True)
    print ("Created error database")
    dataactcore.scripts.setupStaging
    print ("Created staging database")
    
if __name__ == '__main__':
    setupAllDB()
