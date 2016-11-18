import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import sumNumberOfErrorsForJobList
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import (
    Job, JobStatus, Submission, FileType, PublishStatus)
from dataactcore.models.lookups import JOB_STATUS_DICT, PUBLISH_STATUS_DICT


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

    def getJobStatusNameById(self, status_id):
        """ Returns the status name that corresponds to the given id """
        return self.getNameFromDict(JobStatus,"JOB_STATUS_DICT","name",status_id,"job_status_id")

    def getOriginalFilenameById(self,job_id):
        """ Get original filename for job matching ID """
        sess = GlobalDB.db().session
        return sess.query(Job).filter_by(job_id = job_id).one().original_filename

    def getFileSizeById(self,job_id):
        """ Get file size for job matching ID """
        sess = GlobalDB.db().session
        return sess.query(Job).filter_by(job_id = job_id).one().file_size

    def getNumberOfRowsById(self,job_id):
        """ Get number of rows in file for job matching ID """
        sess = GlobalDB.db().session
        return sess.query(Job).filter_by(job_id = job_id).one().number_of_rows

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
        status_names = JOB_STATUS_DICT.keys()
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

    def populateSubmissionErrorInfo(self, submission_id):
        """Deprecated: moved to function_bag.py."""
        sess = GlobalDB.db().session
        submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
        # TODO find where interfaces is set as an instance variable which overrides the static variable, fix that and then remove this line
        self.interfaces = BaseInterface.interfaces
        submission.number_of_errors = sumNumberOfErrorsForJobList(submission_id)
        submission.number_of_warnings = sumNumberOfErrorsForJobList(submission_id, errorType = "warning")
        self.session.commit()

    def setPublishableFlag(self, submission_id, publishable):
        """ Set publishable flag to specified value """
        sess = GlobalDB.db().session
        submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
        submission.publishable = publishable
        self.session.commit()

    def extract_submission(self, submission_or_id):
        """ If given an integer, get the specified submission, otherwise return the input """
        if isinstance(submission_or_id, int):
            sess = GlobalDB.db().session
            return sess.query(Submission).filter_by(submission_id = submission_or_id).one()
        else:
            return submission_or_id

    def setPublishStatus(self, statusName, submissionOrId):
        """ Set publish status to specified name"""
        statusId = PUBLISH_STATUS_DICT[statusName]
        submission = self.extract_submission(submissionOrId)
        submission.publish_status_id = statusId
        self.session.commit()

    def updatePublishStatus(self, submissionOrId):
        """ If submission was already published, mark as updated.  Also set publishable back to false. """
        submission = self.extract_submission(submissionOrId)
        publishedStatus = PUBLISH_STATUS_DICT["published"]
        if submission.publish_status_id == publishedStatus:
            # Submission already published, mark as updated
            self.setPublishStatus("updated", submission)
        # Changes have been made, so don't publish until user marks as publishable again
        submission.publishable = False
        self.session.commit()

