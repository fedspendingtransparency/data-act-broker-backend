import sqlalchemy
import json
from baseInterface import BaseInterface

class JobTrackerInterface(BaseInterface):
    """ Manages all interaction with the job tracker database
    """
    dbConfigFile = "jobTracker.conf"
    dbName = "job_tracker"
    # May or may not need constructor here to define config file location
    #def __init__(self):
        #""" Set up connection to job tracker database """
        #uper.__init__()


    def getSession(self):
        return self.session