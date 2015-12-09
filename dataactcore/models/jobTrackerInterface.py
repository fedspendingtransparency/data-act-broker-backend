import sqlalchemy
import json
from baseInterface import BaseInterface
import os
import inspect

class JobTrackerInterface(BaseInterface):
    """ Manages all interaction with the job tracker database
    """
    dbConfigFile = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + "/jobTracker.conf" #"jobTracker.conf"
    dbName = "job_tracker"
    # May or may not need constructor here to define config file location
    #def __init__(self):
        #""" Set up connection to job tracker database """
        #self.dbConfigFile =
        #super.__init__()


    def getSession(self):
        return self.session