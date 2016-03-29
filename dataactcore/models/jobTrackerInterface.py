import traceback
from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.utils import jobQueue

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
        print "DEBUG: Job status query status name => " + str(self.checkJobUnique(query).status.name)
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

    def markStatus(self,jobId,statusName):
        # Pull JobStatus for jobId
        print "DEBUG: Marking status as " + statusName
        prevStatus = self.getJobStatus(jobId)

        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        # Mark it finished
        result.status_id = self.getStatusId(statusName)
        print "DEBUG: Status marked with status id " + str(self.getStatusId(statusName))
        # Push
        self.session.commit()

        # If status is changed to finished for the first time, check dependencies
        # and add to the job queue as necessary
        if prevStatus != 'finished' and statusName == 'finished':
            self.checkJobDependencies(jobId)

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
        return self.getIdFromDict(Status,"STATUS_DICT","name",statusName,"status_id")

    def getTypeId(self,typeName):
        return self.getIdFromDict(Type,"TYPE_DICT","name",typeName,"type_id")

    def getPrerequisiteJobs(self, jobId):
        """

        Args:
            jobId: job to get dependent jobs of
        Returns:
            list of prerequisite jobs for the specified job
        """
        prerequisiteJobs = []
        queryResult = self.session.query(JobDependency.prerequisite_id).filter(JobDependency.job_id == jobId).all()
        for result in queryResult:
            prerequisiteJobs.append(result.prerequisite_id)
        return prerequisiteJobs

    def checkJobDependencies(self,jobId):
        # raise exception if current job is not actually finished
        if self.getStatus(jobId) != self.getStatusId('finished'):
            raise ValueError('Current job not finished, unable to check dependencies')

        # check if dependent jobs are finished
        for depJobId in self.getDependentJobs(jobId):
            print "DEBUG: Processing dep id => " + str(depJobId)
            isReady = True
            if not (self.getStatus(depJobId) == self.getStatusId('waiting')):
                CloudLogger.logError("Job dependency is not in a 'waiting' state",
                                     ResponseException("Job dependency is not in a 'waiting' state",StatusCode.CLIENT_ERROR, ValueError),
                                     traceback.extract_stack())
                continue
            # if dependent jobs are finished, then check the jobs of which the current job is a dependent
            for preReqJobId in self.getPrerequisiteJobs(depJobId):
                print "DEBUG: Processing prereq job id => " + str(preReqJobId)
                if not (self.getStatus(preReqJobId) == self.getStatusId('finished')):
                    # Do nothing
                    isReady = False
                    break
            if isReady:
                # mark job as ready
                self.markStatus(depJobId, 'ready')
                # add to the job queue
                print("DEBUG: Enqueueing JOB ID => " + str(depJobId))
                x = jobQueue.enqueue.delay(depJobId)
                print "DEBUG: async job result => " + str(x.result)