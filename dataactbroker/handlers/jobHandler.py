from datetime import datetime
from dataactcore.models.jobModels import JobStatus,JobDependency,Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

class JobHandler(JobTrackerInterface):
    """ Responsible for all interaction with the job tracker database

    Instance fields:
    engine -- sqlalchemy engine for generating connections and sessions
    connection -- sqlalchemy connection for executing direct SQL statements
    session -- sqlalchemy session for ORM usage
    waitingStatus -- status_id for "waiting"
    runningStatus -- status_id for "running"
    finishedStatus -- status_id for "finished"
    fileUploadType -- type_id for "file_upload"
    dbUploadType -- type_id for "db_upload"
    validationType -- type_id for "validation"
    externalValidationType -- type_id for "external_validation"
    """

    # Available instance variables:  session, waitingStatus, runningStatus, fileUploadType, dbUploadType, validationType, externalValidationTYpe

    def getSubmissionById(self,submissionId):
        """ Return submission object that matches ID """
        query = self.session.query(Submission).filter(Submission.submission_id == submissionId)
        result = self.runUniqueQuery(query,"No submission with that ID","Multiple submissions with that ID")
        return result

    def getSubmissionsByUserId(self,userId):
        """ Returns all submissions associated with the specified user ID """
        return self.session.query(Submission).filter(Submission.user_id == userId).all()

    def getSubmissionsByUser(self,user):
        """ Returns all submissions associated with the provided user object """
        return self.getSubmissionsByUserId(user.user_id)

    def createJobs(self,filenames,userId):
        """  Given the filenames to be uploaded, create the set of jobs needing to be completed for this submission

        Arguments:
        filenames -- List of filenames to be uploaded
        userId -- User ID to be linked to submission

        Returns:
        Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """
        # Create submission entry
        submission = Submission(datetime_utc = str(datetime.utcnow()))
        submission.user_id = userId
        self.session.add(submission)
        self.session.commit()
        # Calling submission_id to force query to load this
        submission.submission_id

        jobsRequired, uploadDict = self.addUploadJobs(filenames,submission)

        # Create validation job
        validationJob = JobStatus(status_id = self.getStatusId("waiting"), type_id = self.getTypeId("validation"), submission_id = submission.submission_id)
        self.session.add(validationJob)
        # Create external validation job
        externalJob = JobStatus(status_id = self.getStatusId("waiting"), type_id = self.getTypeId("external_validation"), submission_id = submission.submission_id)
        self.session.add(externalJob)
        self.session.flush()
        # Create dependencies for validation jobs
        for job_id in jobsRequired:
            valDependency = JobDependency(job_id = validationJob.job_id, prerequisite_id = job_id)
            self.session.add(valDependency)
            extDependency = JobDependency(job_id = externalJob.job_id, prerequisite_id = job_id)
            self.session.add(extDependency)

        # Commit all changes
        self.session.commit()
        uploadDict["submission_id"] = submission.submission_id
        return uploadDict

    def addUploadJobs(self,filenames,submission):
        """  Add upload jobs to job tracker database

        Arguments:
        filenames -- List of filenames to be uploaded

        Returns:
        jobsRequired -- List of job ids required for validation jobs, used to populate the prerequisite table
        uploadDict -- Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """

        # Keep list of job ids required for validation jobs
        jobsRequired = []
        # Dictionary of upload ids by filename to return to client
        uploadDict = {}



        for fileType, filename in filenames:
            fileTypeQuery = self.session.query(FileType.file_type_id).filter(FileType.name == fileType)
            fileTypeResult = self.runUniqueQuery(fileTypeQuery,"No matching file type", "Multiple matching file types")
            fileTypeId = fileTypeResult.file_type_id

            # Create upload job, mark as running since frontend should be doing this upload
            fileJob = JobStatus(filename = filename, file_type_id = fileTypeId, status_id = self.getStatusId("running"), type_id = self.getTypeId("file_upload"), submission_id = submission.submission_id)

            self.session.add(fileJob)

            # Create parse into DB job
            dbJob = JobStatus(filename = filename, file_type_id = fileTypeId, status_id = self.getStatusId("waiting"), type_id = self.getTypeId("csv_record_validation"), submission_id = submission.submission_id)
            self.session.add(dbJob)
            self.session.flush()
            # Add dependency between file upload and db upload
            uploadDependency = JobDependency(job_id = dbJob.job_id, prerequisite_id = fileJob.job_id)
            self.session.add(uploadDependency)
            # Later validation jobs are dependent only on record level validation, not upload jobs
            jobsRequired.append(dbJob.job_id)
            uploadDict[fileType] = fileJob.job_id

        # Return list of upload jobs
        return jobsRequired, uploadDict

    def checkUploadType(self, jobId):
        """ Check that specified job is a file_upload job

        Args:
        jobId -- Job ID to check
        Returns:
        True if file upload, False otherwise
        """
        query = self.session.query(JobStatus.type_id).filter(JobStatus.job_id == jobId)
        result = self.checkJobUnique(query)
        # Got single job, check type
        if(result.type_id == self.getTypeId("file_upload")):
            # Correct type
            return True
        # Did not confirm correct type
        return False

    def changeToFinished(self, jobId):
        """  Mark an upload job as finished

        Arguments:
        jobId -- job_id to mark as finished

        """

        # Pull from job status table
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(len(queryResult) != 1):
            # Did not find a unique match to job ID
            raise ValueError("Job ID not found")
        jobToChange = queryResult[0]
        # Change status to finished
        jobToChange.status_id = self.getStatusId("finished")
        # Commit changes
        self.session.commit()

    def getUserForSubmission(self,submission):
        """ Takes a submission object and returns the user ID """
        return submission.user_id

    def getSubmissionForJob(self,job):
        """ Takes a job about and returns the associated submission object """
        query = self.session.query(Submission).filter(Submission.submission_id == job.submission_id)
        try:
            result = self.runUniqueQuery(query,"This job has no attached submission", "Multiple submissions with conflicting ID")
            return result
        except ResponseException as e:
            # Either of these errors is a 500, jobs should not be created without being part of a submission
            e.status = StatusCode.INTERNAL_ERROR
            raise e

    def getJobById(self,jobId):
        """ Given a job ID, return the corresponding job """
        query = self.session.query(JobStatus).filter(JobStatus.job_id == jobId)
        result = self.runUniqueQuery(query,"No job with that ID","Multiple jobs with conflicting ID")
        return result