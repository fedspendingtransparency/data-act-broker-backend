import sqlalchemy
import json
from dataactcore.models.baseInterface import BaseInterface
import os
import inspect
from dataactcore.utils.responseException import ResponseException
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound

class JobTrackerInterface(BaseInterface):
    """ Manages all interaction with the job tracker database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """
    dbName = "job_tracker"

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(JobTrackerInterface,self).__init__()

    @staticmethod
    def getCredFilePath():
        """  Returns full path to credentials file """
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        lastBackSlash = path.rfind("\\",0,-1)
        lastForwardSlash = path.rfind("/",0,-1)
        lastSlash = max([lastBackSlash,lastForwardSlash])
        return path[0:lastSlash] + "/credentials/dbCred.json" #"dbCred.json"

    @staticmethod
    def getDbName():
        """ Return database name"""
        return JobTrackerInterface.dbName

    @staticmethod
    def checkJobUnique(queryResult):
        """ Checks if sqlalchemy queryResult has only one entry, error messages are specific to unique jobs

        Args:
        queryResult -- sqlalchemy query result

        Returns:
        True if single result, otherwise exception
        """
        # TODO move this code to a more general function in BaseInterface that takes error messages as arguments
        if(len(queryResult) == 0):
                # Did not get a result for this job
            exc = ResponseException("Job ID not found in job_status table")
            exc.status = 400
            exc.wrappedException = NoResultFound("Job ID not found in job_status table")
            raise exc
        elif(len(queryResult) > 1):
            # Multiple results for single job ID
            exc = ResponseException("Conflicting jobs found for this ID")
            exc.status = 400
            exc.wrappedException = MultipleResultsFound("Conflicting jobs found for this ID")
            raise exc
        return True

    def getSession(self):
        return self.session

