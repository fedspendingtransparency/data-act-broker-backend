import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import sumNumberOfErrorsForJobList
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import Job, JobStatus
from dataactcore.models.lookups import JOB_STATUS_DICT


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


