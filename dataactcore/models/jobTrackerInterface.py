import traceback
from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import Job, JobDependency, JobStatus, JobType, Submission
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.utils.jobQueue import JobQueue
from dataactcore.config import CONFIG_DB


class JobTrackerInterface(BaseInterface):
    """Manages all interaction with the job tracker database."""
    dbConfig = CONFIG_DB
    dbName = dbConfig['job_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['job_db_name']
        self.jobQueue = JobQueue()
        super(JobTrackerInterface, self).__init__()

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
        return BaseInterface.runUniqueQuery(query, "Job ID not found in job table","Conflicting jobs found for this ID")

    def getSession(self):
        """ Return session object"""
        return self.session

    def getJobById(self,jobId):
        """ Return job model object based on ID """
        query = self.session.query(Job).filter(Job.job_id == jobId)
        return self.checkJobUnique(query)

    def getFileName(self,jobId):
        """ Get filename listed in database for this job """
        return self.getJobById(jobId).filename

    def getFileType(self,jobId):
        """ Get type of file associated with this job """
        query = self.session.query(Job).options(joinedload("file_type")).filter(Job.job_id == jobId)
        return self.checkJobUnique(query).file_type.name

    def getFileSize(self,jobId):
        """ Get size of the file associated with this job """
        return self.getJobById(jobId).file_size

    def getSubmissionId(self,jobId):
        """ Find submission that this job is part of """
        return self.getJobById(jobId).submission_id

    def getSubmission(self, jobId):
        """ Return submission object """
        submissionId = self.getSubmissionId(jobId)
        return self.session.query(Submission).filter(Submission.submission_id == submissionId).one()

    def getReportPath(self,jobId):
        """ Return the filename for the error report.  Does not include the folder to avoid conflicting with the S3 getSignedUrl method. """
        return  "submission_" + str(self.getSubmissionId(jobId)) + "_" + self.getFileType(jobId) + "_error_report.csv"

    def getCrossFileReportPath(self,submissionId):
        """ Returns the filename for the cross file error report. """
        return "".join(["submission_",str(submissionId),"_cross_file_error_report.csv"])

    def getJobsBySubmission(self,submissionId):
        """ Get list of jobs that are part of the specified submission

        Args:
            submissionId: submission to list jobs for

        Returns:
            List of job IDs
        """
        jobList = []
        queryResult = self.session.query(Job.job_id).filter(Job.submission_id == submissionId).all()
        for result in queryResult:
            jobList.append(result.job_id)
        return jobList

    def getJobStatusName(self, jobId):
        """

        Args:
            jobId: Job status to get

        Returns:
            status name of specified job
        """
        query = self.session.query(Job).options(joinedload("job_status")).filter(Job.job_id == jobId)
        return self.checkJobUnique(query).job_status.name

    def getJobType(self, jobId):
        """

        Args:
            jobId: Job to get description for

        Returns:
            description of specified job
        """

        query = self.session.query(Job).options(joinedload("job_type")).filter(Job.job_id == jobId)
        return self.checkJobUnique(query).job_type.name

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

    def markJobStatus(self,jobId,statusName):
        """ Mark job as having specified status.  Jobs being marked as finished will add dependent jobs to queue.

        Args:
            jobId: ID for job being marked
            statusName: Status to change job to
        """
        # Pull JobStatus for jobId
        prevStatus = self.getJobStatusName(jobId)

        query = self.session.query(Job).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        # Mark it finished
        result.job_status_id = self.getJobStatusId(statusName)
        # Push
        self.session.commit()

        # If status is changed to finished for the first time, check dependencies
        # and add to the job queue as necessary
        if prevStatus != 'finished' and statusName == 'finished':
            self.checkJobDependencies(jobId)

    def getJobStatusNames(self):
        """ Get All Job Status names """

        # This populates the DICT
        self.getJobStatusId(None)
        return JobStatus.JOB_STATUS_DICT.keys()

    def getJobStatus(self,jobId):
        """ Get status for specified job

        Args:
        jobId -- job to get status for

        Returns:
        status ID
        """
        status = None
        query = self.session.query(Job.job_status_id).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        status = result.job_status_id
        self.session.commit()
        return status

    def getJobStatusId(self,statusName):
        """ Return the status ID that corresponds to the given name """
        return self.getIdFromDict(
            JobStatus, "JOB_STATUS_DICT", "name", statusName, "job_status_id")

    def getJobTypeId(self,typeName):
        """ Return the type ID that corresponds to the given name """
        return self.getIdFromDict(JobType,"JOB_TYPE_DICT","name",typeName,"job_type_id")

    def getOriginalFilenameById(self,jobId):
        """ Get original filename for job matching ID """
        return self.getJobById(jobId).original_filename

    def getPrerequisiteJobs(self, jobId):
        """
        Get all the jobs of which the current job is a dependent

        Args:
            jobId: job to get dependent jobs of
        Returns:
            list of prerequisite jobs for the specified job
        """
        queryResult = self.session.query(JobDependency.prerequisite_id).filter(JobDependency.job_id == jobId).all()
        prerequisiteJobs = [result.prerequisite_id for result in queryResult]
        return prerequisiteJobs

    def checkJobDependencies(self,jobId):
        """ For specified job, check which of its dependencies are ready to be started, and add them to the queue """

        # raise exception if current job is not actually finished
        if self.getJobStatus(jobId) != self.getJobStatusId('finished'):
            raise ValueError('Current job not finished, unable to check dependencies')

        # check if dependent jobs are finished
        for depJobId in self.getDependentJobs(jobId):
            isReady = True
            if not (self.getJobStatus(depJobId) == self.getJobStatusId('waiting')):
                CloudLogger.logError("Job dependency is not in a 'waiting' state",
                                     ResponseException("Job dependency is not in a 'waiting' state",StatusCode.CLIENT_ERROR, ValueError),
                                     traceback.extract_stack())
                continue
            # if dependent jobs are finished, then check the jobs of which the current job is a dependent
            for preReqJobId in self.getPrerequisiteJobs(depJobId):
                if not (self.getJobStatus(preReqJobId) == self.getJobStatusId('finished')):
                    # Do nothing
                    isReady = False
                    break
            # The type check here is temporary and needs to be removed once the validator is able
            # to handle cross-file validation job
            if isReady and (self.getJobType(depJobId) == 'csv_record_validation' or self.getJobType(depJobId) == 'validation'):
                # mark job as ready
                self.markJobStatus(depJobId, 'ready')
                # add to the job queue
                jobQueueResult = self.jobQueue.enqueue.delay(depJobId)

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

    def getSubmissionStatus(self,submissionId):
        jobIds = self.getJobsBySubmission(submissionId)
        status_names = self.getJobStatusNames()
        statuses = dict(zip(status_names,[0]*len(status_names)))
        skip_count = 0

        for jobId in jobIds:
            job = self.getJobById(jobId)
            if job.job_type.name != "external_validation":
                job_status = job.job_status.name
                statuses[job_status] += 1
            else:
                skip_count += 1

        if statuses["failed"] != 0:
            return "failed"
        if statuses["invalid"] != 0:
            return "invalid"
        if statuses["running"] != 0:
            return "running"
        if statuses["waiting"] != 0:
            return "waiting"
        if statuses["ready"] != 0:
            return "ready"
        if statuses["finished"] == len(jobIds)-skip_count: # need to account for the jobs that were skipped above
            return "finished"
        return "unknown"
