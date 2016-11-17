import logging

from sqlalchemy.orm import joinedload

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import sumNumberOfErrorsForJobList
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import (
    Job, JobStatus, JobType, Submission, FileType,
    PublishStatus)


_exception_logger = logging.getLogger('deprecated.exception')


class JobTrackerInterface(BaseInterface):
    """Manages all interaction with the job tracker database."""
    def checkJobUnique(self, query):
        """ Checks if sqlalchemy queryResult has only one entry, error messages are specific to unique jobs

        Args:
        queryResult -- sqlalchemy query result

        Returns:
        True if single result, otherwise exception
        """
        return self.runUniqueQuery(query, "Job ID not found in job table","Conflicting jobs found for this ID")

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

    def getSubmissionId(self,jobId):
        """ Find submission that this job is part of """
        return self.getJobById(jobId).submission_id

    def getJobType(self, jobId):
        """

        Args:
            jobId: Job to get description for

        Returns:
            description of specified job
        """

        query = self.session.query(Job).options(joinedload("job_type")).filter(Job.job_id == jobId)
        return self.checkJobUnique(query).job_type.name

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
        query = self.session.query(Job.job_status_id).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        status = result.job_status_id
        self.session.commit()
        return status

    def getJobStatusId(self,statusName):
        """ Return the status ID that corresponds to the given name """
        return self.getIdFromDict(
            JobStatus, "JOB_STATUS_DICT", "name", statusName, "job_status_id")

    def getJobStatusNameById(self, status_id):
        """ Returns the status name that corresponds to the given id """
        return self.getNameFromDict(JobStatus,"JOB_STATUS_DICT","name",status_id,"job_status_id")

    def getJobTypeId(self,typeName):
        """ Return the type ID that corresponds to the given name """
        return self.getIdFromDict(JobType,"JOB_TYPE_DICT","name",typeName,"job_type_id")

    def getFileTypeId(self, typeName):
        """ Returns the file type id that corresponds to the given name """
        return self.getIdFromDict(FileType, "FILE_TYPE_DICT", "name", typeName, "file_type_id")

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

    def setJobRowcounts(self, jobId, numRows, numValidRows):
        """Set number of rows in job that passed validations."""
        self.session.query(Job).filter(Job.job_id == jobId).update(
            {"number_of_rows_valid": numValidRows, "number_of_rows": numRows})
        self.session.commit()

    def getSubmissionStatus(self,submission):
        # obviously this entire file is going away soon, but temporarily
        # patch this so we can remove getJobsBySubmission function
        sess = GlobalDB.db().session
        jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)
        status_names = self.getJobStatusNames()
        statuses = dict(zip(status_names,[0]*len(status_names)))
        skip_count = 0

        for job in jobs:
            if job.job_type.name not in ["external_validation", None]:
                job_status = job.job_status.name
                statuses[job_status] += 1
            else:
                skip_count += 1

        status = "unknown"

        if statuses["failed"] != 0:
            status = "failed"
        elif statuses["invalid"] != 0:
            status = "file_errors"
        elif statuses["running"] != 0:
            status = "running"
        elif statuses["waiting"] != 0:
            status = "waiting"
        elif statuses["ready"] != 0:
            status = "ready"
        elif statuses["finished"] == jobs.count()-skip_count: # need to account for the jobs that were skipped above
            status = "validation_successful"
            if submission.number_of_warnings is not None and submission.number_of_warnings > 0:
                status = "validation_successful_warnings"
            if submission.publishable:
                status = "submitted"


        # Check if submission has errors
        if submission.number_of_errors is not None and submission.number_of_errors > 0:
            status = "validation_errors"

        return status

    def getSubmissionById(self, submissionId):
        """ Return submission object that matches ID"""
        query = self.session.query(Submission).filter(Submission.submission_id == submissionId)
        return self.runUniqueQuery(query, "No submission with that ID", "Multiple submissions with that ID")

    def populateSubmissionErrorInfo(self, submission_id):
        """Deprecated: moved to function_bag.py."""
        submission = self.getSubmissionById(submission_id)
        # TODO find where interfaces is set as an instance variable which overrides the static variable, fix that and then remove this line
        self.interfaces = BaseInterface.interfaces
        submission.number_of_errors = sumNumberOfErrorsForJobList(submission_id)
        submission.number_of_warnings = sumNumberOfErrorsForJobList(submission_id, errorType = "warning")
        self.session.commit()

    def setJobNumberOfErrors(self, jobId, numberOfErrors, errorType):
        """Deprecated: moved to sumNumberOfErrorsForJobList in function_bag.py."""
        job = self.getJobById(jobId)
        if errorType == "fatal":
            job.number_of_errors = numberOfErrors
        elif errorType == "warning":
            job.number_of_warnings = numberOfErrors
        self.session.commit()

    def setPublishableFlag(self, submissionId, publishable):
        """ Set publishable flag to specified value """
        submission = self.getSubmissionById(submissionId)
        submission.publishable = publishable
        self.session.commit()

    def extractSubmission(self, submissionOrId):
        """ If given an integer, get the specified submission, otherwise return the input """
        if isinstance(submissionOrId, int):
            return self.getSubmissionById(submissionOrId)
        else:
            return submissionOrId

    def setPublishStatus(self, statusName, submissionOrId):
        """ Set publish status to specified name"""
        statusId = self.getPublishStatusId(statusName)
        submission = self.extractSubmission(submissionOrId)
        submission.publish_status_id = statusId
        self.session.commit()

    def updatePublishStatus(self, submissionOrId):
        """ If submission was already published, mark as updated.  Also set publishable back to false. """
        submission = self.extractSubmission(submissionOrId)
        publishedStatus = self.getPublishStatusId("published")
        if submission.publish_status_id == publishedStatus:
            # Submission already published, mark as updated
            self.setPublishStatus("updated", submission)
        # Changes have been made, so don't publish until user marks as publishable again
        submission.publishable = False
        self.session.commit()

    def getPublishStatusId(self, statusName):
        """ Return ID for specified publish status """
        return self.getIdFromDict(PublishStatus,  "PUBLISH_STATUS_DICT", "name", statusName, "publish_status_id")

