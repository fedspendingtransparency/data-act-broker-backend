from datetime import date
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.jobModels import Job, JobDependency
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.validation_handlers.validationError import ValidationError

class ValidatorJobTrackerInterface(JobTrackerInterface):
    """ Manages all interaction with the job tracker database """

    def runChecks(self,jobId):
        """ Run all checks on this jobId

        Args:
        jobId -- ID of job to be checked
        Returns:
        True if all passed, otherwise False or exception
        """
        if(self.checkJobReady(jobId) and self.checkPrerequisites(jobId)):
            # All passed
            return True
        else:
            return False

    def checkJobReady(self, jobId):
        """ Check that the jobId is located in job table and that status is ready
        Args:
        jobId -- ID of job to be run

        Returns:
        True if job is ready, False otherwise
        """
        query = self.session.query(Job.job_status_id).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        # Found a unique job
        if(result.job_status_id != self.getJobStatusId("ready")):
            # Job is not ready
            # Job manager is not yet implemented, so for now doesn't have to be ready
            return True
            # TODO when job manager exists, change to exception below
            #exc = ResponseException("Job is not ready",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)
            #exc.status = 400
            #raise exc

        return True

    def checkPrerequisites(self, jobId):
        """ Checks that specified job has no unsatisfied prerequisites
        Args:
        jobId -- job_id of job to be run

        Returns:
        True if prerequisites are satisfied, False otherwise
        """
        # Get list of prerequisites
        queryResult = self.session.query(JobDependency).filter(JobDependency.job_id == jobId).all()
        for prereq in queryResult:
            query = self.session.query(Job).filter(Job.job_id == prereq.prerequisite_id)
            result = self.checkJobUnique(query)
            # Found a unique job
            if(result.job_status_id != self.getJobStatusId("finished")):
                # Prerequisite not complete
                raise ResponseException("Prerequisites incomplete, job cannot be started",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)

        return True

    def checkJobType(self,jobId):
        """ Job should be of type csv_record_validation, or this is the wrong service

        Args:
        jobId -- job ID to check

        Returns:
        True if correct type, False or exception otherwise
        """
        query = self.session.query(Job.job_type_id).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        if result.job_type_id == self.getJobTypeId("csv_record_validation") or result.job_type_id == self.getJobTypeId("validation"):
            # Correct type
            return result.job_type_id
        else:
            # Wrong type
            raise ResponseException("Wrong type of job for this service",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)

    def checkFirstQuarter(self,jobId):
        """ Return True if end date is in the first quarter """
        submission = self.getSubmission(jobId)
        endDate = submission.reporting_end_date
        if endDate is None:
            # No date provided, consider this to not be first quarter
            return False
        return (endDate.month >= 10 and endDate.month <= 12)
