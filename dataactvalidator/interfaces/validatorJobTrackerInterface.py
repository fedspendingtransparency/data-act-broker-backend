from dataactcore.models import jobTrackerInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type, Resource
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.validation_handlers.validationError import ValidationError

class ValidatorJobTrackerInterface(jobTrackerInterface.JobTrackerInterface):
    """ Manages all interaction with the job tracker database """

    def runChecks(self,jobId):
        """ Run all checks on this jobId

        Args:
        jobId -- ID of job to be checked
        Returns:
        True if all passed, otherwise False or exception
        """
        if(self.checkJobReady(jobId) and self.checkPrerequisites(jobId) and self.checkJobType(jobId)):
            # All passed
            return True
        else:
            return False

    def checkJobReady(self, jobId):
        """ Check that the jobId is located in job_status table and that status is ready
        Args:
        jobId -- ID of job to be run

        Returns:
        True if job is ready, False otherwise
        """
        query = self.session.query(JobStatus.status_id).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        # Found a unique job
        if(result.status_id != Status.getStatus("ready")):
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
            query = self.session.query(JobStatus).filter(JobStatus.job_id == prereq.prerequisite_id)
            result = self.checkJobUnique(query)
            # Found a unique job
            if(result.status_id != Status.getStatus("finished")):
                # Prerequisite not complete
                raise ResponseException("Prerequisites incomplete, job cannot be started",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)

        return True

    def addStagingTable(self,jobId,stagingTable):
        """

        Args:
            jobId: Job to write table name for
            stagingTable: Name of staging table for this job

        Returns:
            True if successful
        """
        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        result.staging_table = stagingTable
        self.session.commit()
        return True

    def checkJobType(self,jobId):
        """ Job should be of type csv_record_validation, or this is the wrong service

        Args:
        jobId -- job ID to check

        Returns:
        True if correct type, False or exception otherwise
        """
        query = self.session.query(JobStatus.type_id).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        if(result.type_id == Type.getType("csv_record_validation")):
            # Correct type
            return True
        else:
            # Wrong type
            raise ResponseException("Wrong type of job for this service",StatusCode.CLIENT_ERROR,None,ValidationError.jobError)
