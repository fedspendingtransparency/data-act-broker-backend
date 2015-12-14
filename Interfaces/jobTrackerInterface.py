import sqlalchemy
import json
from dataactcore.models import jobTrackerInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type, Resource
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
from dataactcore.utils.responseException import ResponseException

class JobTrackerInterface(jobTrackerInterface.JobTrackerInterface):
    """ Manages all interaction with the job tracker database
    """

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
        queryResult = self.session.query(JobStatus.status_id).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            # Found a unique job
            if(queryResult[0].status_id != Status.getStatus("ready")):
                # Job is not ready
                # Job manager is not yet implemented, so for now doesn't have to be ready
                return True
                # TODO when job manager exists, change to exception below
                #exc = ResponseException("Job is not ready")
                #exc.status = 400
                #raise exc
        else:
            exc = ResponseException("Job ID not found")
            exc.status = 400
            raise exc
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
            statusResult = self.session.query(JobStatus).filter(JobStatus.job_id == prereq.prerequisite_id).all()
            if(self.checkJobUnique(statusResult)):
                # Found a unique job
                if(statusResult[0].status_id != Status.getStatus("finished")):
                    # Prerequisite not complete
                    exc = ResponseException("Prerequisites incomplete, job cannot be started")
                    exc.status = 400
                    raise exc
        return True

    def markFinished(self,jobId):
        # Pull JobStatus for jobId
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            # Mark it finished
            queryResult[0].status_id = Status.getStatus("finished")
            # Push
            self.session.commit()

    def checkJobType(self,jobId):
        """ Job should be of type csv_record_validation, or this is the wrong service

        Args:
        jobId -- job ID to check

        Returns:
        True if correct type, False or exception otherwise
        """
        queryResult = self.session.query(JobStatus.type_id).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            if(queryResult[0].type_id == Type.getType("csv_record_validation")):
                # Correct type
                return True
            else:
                # Wrong type
                exc = ResponseException("Wrong type of job for this service")
                exc.status = 400
                raise exc

    def getStatus(self,jobId):
        """ Get status for specified job

        Args:
        jobId -- job to get status for

        Returns:
        status ID
        """

        queryResult = self.session.query(JobStatus.status_id).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].status_id

    def addStagingTable(self,jobId,stagingTable):
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            queryResult[0].staging_table = stagingTable
            self.session.commit()
            return True

    def getFileName(self,jobId):
        queryResult = self.session.query(JobStatus.filename).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].filename

    def getFileType(self,jobId):
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(self.checkJobUnique(queryResult)):
            return queryResult[0].file_type.name