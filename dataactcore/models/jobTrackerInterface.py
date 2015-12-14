import sqlalchemy
import json
from dataactcore.models.baseInterface import BaseInterface
import os
import inspect
from dataactcore.utils.responseException import ResponseException


class JobTrackerInterface(BaseInterface):
    """ Manages all interaction with the job tracker database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """
    dbName = "job_tracker"
    credFileName = "dbCred.json"

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(JobTrackerInterface,self).__init__()



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
        return BaseInterface.checkUnique(queryResult, "Job ID not found in job_status table","Conflicting jobs found for this ID")


    def getSession(self):
        return self.session

