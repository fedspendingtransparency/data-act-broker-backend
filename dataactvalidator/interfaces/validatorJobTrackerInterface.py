from datetime import date
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.jobModels import Job, JobDependency
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.validation_handlers.validationError import ValidationError

class ValidatorJobTrackerInterface(JobTrackerInterface):
    """ Manages all interaction with the job tracker database """



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
