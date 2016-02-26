from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type

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
    def checkJobUnique(query):
        """ Checks if sqlalchemy queryResult has only one entry, error messages are specific to unique jobs

        Args:
        queryResult -- sqlalchemy query result

        Returns:
        True if single result, otherwise exception
        """
        return BaseInterface.runUniqueQuery(query, "Job ID not found in job_status table","Conflicting jobs found for this ID")


    def getSession(self):
        """ Return session object"""
        return self.session

    def getFileName(self,jobId):
        """ Get filename listed in database for this job """
        query = self.session.query(JobStatus.filename).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).filename

    def getFileType(self,jobId):
        """ Get type of file associated with this job """
        query = self.session.query(JobStatus).options(joinedload("file_type")).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).file_type.name

    def getSubmissionId(self,jobId):
        """ Find submission that this job is part of """
        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).submission_id

    def getReportPath(self,jobId):
        """ Return the filename for the error report.  Does not include the folder to avoid conflicting with the S3 getSignedUrl method. """
        return  "submission_" + str(self.getSubmissionId(jobId)) + "_" + self.getFileType(jobId) + "_error_report.csv"

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
        query = self.session.query(JobStatus).options(joinedload("status")).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).status.name

    def getJobType(self, jobId):
        """

        Args:
            jobId: Job to get description for

        Returns:
            description of specified job
        """

        query = self.session.query(JobStatus).options(joinedload("type")).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).type.name

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

        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        # Mark it finished
        result.status_id = self.getStatusId(statsType)
        # Push
        self.session.commit()

    def getStatus(self,jobId):
        """ Get status for specified job

        Args:
        jobId -- job to get status for

        Returns:
        status ID
        """
        status = None
        query = self.session.query(JobStatus.status_id).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        status = result.status_id
        self.session.commit()
        return status

    def getStatusId(self,statusName):
        if(Status.STATUS_DICT == None):
            Status.STATUS_DICT = {}
            # Pull status values out of DB
            # Create new session for this
            queryResult = self.session.query(Status).all()
            for status in queryResult:
                Status.STATUS_DICT[status.name] = status.status_id
        if(not statusName in Status.STATUS_DICT):
            raise ValueError("Not a valid job status: " + str(statusName) + ", not found in dict: " + str(Status.STATUS_DICT))
        return Status.STATUS_DICT[statusName]

    def getTypeId(self,typeName):
        if(Type.TYPE_DICT == None):
            Type.TYPE_DICT = {}
            # Pull status values out of DB
            for jobType in Type.TYPE_LIST:
                Type.TYPE_DICT[jobType] = self.setTypeId(jobType)
        if(not typeName in Type.TYPE_DICT):
            raise ValueError("Not a valid job type")
        return Type.TYPE_DICT[typeName]

    def setTypeId(self,name):
        """  Get an id for specified type, if not unique throw an exception

        Arguments:
        name -- Name of type to get an id for

        Returns:
        type_id of the specified type
        """
        # Create new session for this
        queryResult = self.session.query(Type.type_id).filter(Type.name==name).one()
        return queryResult.type_id
