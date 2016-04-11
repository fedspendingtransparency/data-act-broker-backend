from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type
from dataactcore.config import CONFIG_DB


class JobTrackerInterface(BaseInterface):
    """Manages all interaction with the job tracker database."""
    dbName = CONFIG_DB['job_db_name']
    dbConfig = CONFIG_DB
    Session = None
    engine = None
    session = None

    def __init__(self):
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

    def getJobById(self,jobId):
        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query)

    def getFileName(self,jobId):
        """ Get filename listed in database for this job """
        return self.getJobById(jobId).filename

    def getFileType(self,jobId):
        """ Get type of file associated with this job """
        query = self.session.query(JobStatus).options(joinedload("file_type")).filter(JobStatus.job_id == jobId)
        return self.checkJobUnique(query).file_type.name

    def getSubmissionId(self,jobId):
        """ Find submission that this job is part of """
        return self.getJobById(jobId).submission_id

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
        """ Return the status ID that corresponds to the given name """
        return self.getIdFromDict(Status,"STATUS_DICT","name",statusName,"status_id")

    def getTypeId(self,typeName):
        """ Return the type ID that corresponds to the given name """
        return self.getIdFromDict(Type,"TYPE_DICT","name",typeName,"type_id")

    def getOriginalFilenameById(self,jobId):
        """ Get original filename for job matching ID """
        return self.getJobById(jobId).original_filename

    def getFileSizeById(self,jobId):
        """ Get file size for job matching ID """
        return self.getJobById(jobId).file_size

    def getNumberOfRowsById(self,jobId):
        """ Get number of rows in file for job matching ID """
        return self.getJobById(jobId).number_of_rows

    def setFileSizeById(self,jobId, fileSize):
        """ Set file size for job matching ID """
        job = self.getJobById(jobId)
        job.file_size = int(fileSize)
        self.session.commit()

    def setNumberOfRowsById(self,jobId, numRows):
        """ Set number of rows in file for job matching ID """
        job = self.getJobById(jobId)
        job.number_of_rows = int(numRows)
        self.session.commit()
