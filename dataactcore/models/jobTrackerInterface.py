import sqlalchemy
import json
from dataactcore.models.baseInterface import BaseInterface
import os
import inspect
from dataactcore.utils.responseException import ResponseException
from dataactcore.models.jobModels import JobStatus
from sqlalchemy.orm import subqueryload, joinedload

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

    def getFileName(self,jobId):
        queryResult = self.session.query(JobStatus.filename).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].filename

    def getFileType(self,jobId):
        queryResult = self.session.query(JobStatus).options(joinedload("file_type")).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].file_type.name

    def getSubmissionId(self,jobId):
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].submission_id

    def getReportPath(self,jobId):
        return "errors/" + "submission_" + self.getSubmissionId(jobId) + "_" + self.getFileType(jobId) + "_error_report"

