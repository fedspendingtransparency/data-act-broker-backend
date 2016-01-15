from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type
from sqlalchemy.orm import subqueryload, joinedload
from dataactcore.utils.responseException import ResponseException

class JobTrackerInterface(BaseInterface):
    """ Manages all interaction with the job tracker database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """
    dbName = "job_tracker"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

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
        """ Return the filename for the error report.  Does not include the folder to avoid conflicting with the S3 getSignedUrl method. """
        try:
            return  "submission_" + str(self.getSubmissionId(jobId)) + "_" + self.getFileType(jobId) + "_error_report.csv"
        except:
            # Bad job ID
            return False

    def getJobsBySubmission(self,submissionId):
        """ Get list of jobs that are part of the specified submission

        Args:
            submissionId: submission to list jobs for

        Returns:
            List of job IDs
        """
        jobList = []
        queryResult = self.session.query(JobStatus.job_id).filter(JobStatus.submission_id == submissionId).all()
        for result in queryResult:
            jobList.append(result.job_id)
        return jobList

    def getJobStatus(self, jobId):
        """

        Args:
            jobId: Job to get status for

        Returns:
            status of specified job
        """
        queryResult = self.session.query(JobStatus).options(joinedload("status")).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].status.name

    def getJobType(self, jobId):
        """

        Args:
            jobId: Job to get description for

        Returns:
            description of specified job
        """

        queryResult = self.session.query(JobStatus).options(joinedload("type")).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].type.name

    def getDependentJobs(self, jobId):
        """

        Args:
            jobId: job to get dependent jobs of
        Returns:
            list of jobs dependent on the specified job
        """

        dependents = []
        queryResult = self.session.query(JobDependency).filter(JobDependency.prerequisite_id == jobId).all()
        for result in queryResult:
            dependents.append(result.job_id)
        return dependents

    def markStatus(self,jobId,statsType):
        # Pull JobStatus for jobId

        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            # Mark it finished
            queryResult[0].status_id = Status.getStatus(statsType)
            # Push
            self.session.commit()

    def checkJobType(self,jobId):
        """ Job should be of type csv_record_validation, or this is the wrong service

        Args:
        jobId -- job ID to check

        Returns:
        True if correct type, False or exception otherwise
        """
        queryResult = self.session.query(JobStatus.type_id).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            if(queryResult[0].type_id == Type.getType("csv_record_validation")):
                # Correct type
                return True
            else:
                # Wrong type
                raise ResponseException("Wrong type of job for this service",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)

    def getStatus(self,jobId):
        """ Get status for specified job

        Args:
        jobId -- job to get status for

        Returns:
        status ID
        """
        status = None
        queryResult = self.session.query(JobStatus.status_id).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            status = queryResult[0].status_id
        self.session.commit()
        return status