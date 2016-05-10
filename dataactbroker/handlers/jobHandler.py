from datetime import datetime, date
from dataactcore.models.jobModels import Job,JobDependency,Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.errorHandler import ErrorHandler

class JobHandler(JobTrackerInterface):
    """ Responsible for all interaction with the job tracker database
    Class fields:
    metaDataFieldMap -- Maps names in the request object to database column names

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

    metaDataFieldMap = {"agency_name":"agency_name","reporting_period_start_date":"reporting_start_date","reporting_period_end_date":"reporting_end_date"}

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

    @classmethod
    def loadSubmitParams(cls,requestDict):
        """ Load params from request, return dictionary of values provided mapped to submission fields """
        # Existing submission ID is optional
        existingSubmission = False
        existingSubmissionId = None
        if requestDict.exists("existing_submission_id"):
            # Agency name and reporting dates are required for new submissions
            existingSubmission = True
            existingSubmissionId = requestDict.getValue("existing_submission_id")

        submissionData = {}
        for key in cls.metaDataFieldMap:
            if requestDict.exists(key):
                if(key == "reporting_period_start_date" or key == "reporting_period_end_date"):
                    # Create a date object from formatted string, assuming "MM/DD/YYYY"
                    try:
                        submissionData[cls.metaDataFieldMap[key]] = JobHandler.createDate(requestDict.getValue(key))
                    except Exception as e:
                        raise ResponseException("Submission dates must be formatted as MM/DD/YYYY, hit error: " + str(e),StatusCode.CLIENT_ERROR,type(e))
                else:
                    submissionData[cls.metaDataFieldMap[key]] = requestDict.getValue(key)
            else:
                if not existingSubmission:
                    raise ResponseException(key + " is required",StatusCode.CLIENT_ERROR,ValueError)
        return submissionData, existingSubmissionId

    @staticmethod
    def createDate(dateString):
        """ Create a date object from a string in "MM/DD/YYYY" """
        if dateString is None:
            return None
        dateParts = dateString.split("/")
        return date(year = int(dateParts[2]),month = int(dateParts[0]),day = int(dateParts[1]))

    def createSubmission(self, userId, requestDict):
        """ Create a new submission

        Arguments:
            userId:  User to associate with this submission
            requestDict:  Dictionary of keys provided in request, may contain "existing_submission_id", "agency_name", "reporting_period_start_date", "reporting_period_end_date"

        Returns:
            submission ID
        """
        # submissionValues is a dictionary with keys determined by JobHandler.metaDataFieldMap, and existingId is the existing submission ID if it exists
        submissionValues,existingId = self.loadSubmitParams(requestDict)
        # Create submission entry
        if existingId is None:
            submission = Submission(datetime_utc = datetime.utcnow(), **submissionValues)
            submission.user_id = userId
            self.session.add(submission)
        else:
            submissionQuery = self.session.query(Submission).filter(Submission.submission_id == existingId)
            submission = self.runUniqueQuery(submissionQuery,"No submission found with provided ID", "Multiple submissions found with provided ID")
            #if "reporting_start_date" in submissionValues:
            #    submission.reporting_start_date = submissionValues["reporting_start_date"]
            for key in submissionValues:
                # Update existing submission with any values provided
                #submission.__dict__[key] = submissionValues[key]
                setattr(submission,key,submissionValues[key])
            self.session.commit()
        self.session.commit()
        # Calling submission_id to force query to load this
        return submission.submission_id

    def createJobs(self, filenames, submissionId, existingSubmission = False):
        """  Given the filenames to be uploaded, create the set of jobs needing to be completed for this submission

        Arguments:
        filenames -- List of tuples containing (file type, upload path, original filenames)
        submissionId -- Submission ID to be linked to jobs
        existingSubmission -- True if we should update jobs in an existing submission rather than creating new jobs

        Returns:
        Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """


        jobsRequired, uploadDict = self.addUploadJobs(filenames,submissionId,existingSubmission)

        if(existingSubmission):
            # Find cross-file and external validation jobs and mark them as waiting
            valQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.type_id == self.getTypeId("validation"))
            valJob = self.runUniqueQuery(valQuery,"No cross-file validation job found","Conflicting jobs found")
            valJob.job_status_id = self.getJobStatusId("waiting")
            extQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.type_id == self.getTypeId("external_validation"))
            extJob = self.runUniqueQuery(valQuery,"No external validation job found","Conflicting jobs found")
            extJob.job_status_id = self.getJobStatusId("waiting")
            self.session.commit()
        else:
            # Create validation job
            validationJob = Job(job_status_id=self.getJobStatusId("waiting"), type_id=self.getTypeId("validation"), submission_id=submissionId)
            self.session.add(validationJob)
            # Create external validation job
            externalJob = Job(job_status_id=self.getJobStatusId("waiting"), type_id=self.getTypeId("external_validation"), submission_id=submissionId)
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
        uploadDict["submission_id"] = submissionId
        return uploadDict

    def addUploadJobs(self,filenames,submissionId,existingSubmission):
        """  Add upload jobs to job tracker database

        Arguments:
        filenames -- List of tuples containing (file type, upload path, original filenames)
        submissionId -- Submission ID to attach to jobs
        existingSubmission -- True if we should update existing jobs rather than creating new ones

        Returns:
        jobsRequired -- List of job ids required for validation jobs, used to populate the prerequisite table
        uploadDict -- Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """

        # Keep list of job ids required for validation jobs
        jobsRequired = []
        # Dictionary of upload ids by filename to return to client
        uploadDict = {}

        for fileType, filePath, filename in filenames:
            fileTypeQuery = self.session.query(FileType.file_type_id).filter(FileType.name == fileType)
            fileTypeResult = self.runUniqueQuery(fileTypeQuery,"No matching file type", "Multiple matching file types")
            fileTypeId = fileTypeResult.file_type_id

            if existingSubmission:
                # Find existing upload job and mark as running
                uploadQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.type_id == self.getTypeId("file_upload"))
                uploadJob = self.runUniqueQuery(uploadQuery,"No upload job found for this file","Conflicting jobs found")
                # Mark as running and set new file name and path
                uploadJob.job_status_id = self.getJobStatusId("running")
                uploadJob.original_filename = filename
                uploadJob.filename = filePath
                self.session.commit()
            else:
                # Create upload job, mark as running since frontend should be doing this upload
                uploadJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId, job_status_id=self.getJobStatusId("running"), type_id=self.getTypeId("file_upload"), submission_id=submissionId)
                self.session.add(uploadJob)

            if existingSubmission:
                valQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.type_id == self.getTypeId("csv_record_validation"))
                valJob = self.runUniqueQuery(valQuery,"No validation job found for this file","Conflicting jobs found")
                valJob.job_status_id = self.getJobStatusId("waiting")
                valJob.original_filename = filename
                valJob.filename = filePath
                # Reset file size and number of rows to be set during validation of new file
                valJob.file_size = None
                valJob.number_of_rows = None
                # Reset number of errors
                errorDb = ErrorHandler()
                errorDb.resetErrorsByJobId(valJob.job_id)
                errorDb.resetFileByJobId(valJob.job_id)
                self.session.commit()
            else:
                # Create parse into DB job
                valJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId, job_status_id=self.getJobStatusId("waiting"), type_id=self.getTypeId("csv_record_validation"), submission_id=submissionId)
                self.session.add(valJob)
                self.session.flush()
            if not existingSubmission:
                # Add dependency between file upload and db upload
                uploadDependency = JobDependency(job_id = valJob.job_id, prerequisite_id = uploadJob.job_id)
                self.session.add(uploadDependency)
                # Later validation jobs are dependent only on record level validation, not upload jobs
                jobsRequired.append(valJob.job_id)
            uploadDict[fileType] = uploadJob.job_id

        # Return list of upload jobs
        return jobsRequired, uploadDict

    def checkUploadType(self, jobId):
        """ Check that specified job is a file_upload job

        Args:
        jobId -- Job ID to check
        Returns:
        True if file upload, False otherwise
        """
        query = self.session.query(Job.type_id).filter(Job.job_id == jobId)
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
        JobTrackerInterface.markJobStatus(self, jobId, 'finished')

    def getUserForSubmission(self,submission):
        """ Takes a submission object and returns the user ID """
        return submission.user_id

    def getSubmissionForJob(self,job):
        """ Takes a job object and returns the associated submission object """
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
        query = self.session.query(Job).filter(Job.job_id == jobId)
        result = self.runUniqueQuery(query,"No job with that ID","Multiple jobs with conflicting ID")
        return result

    def sumNumberOfRowsForJobList(self, jobList):
        """ Given a list of job IDs, return the number of rows summed across jobs """
        rowSum = 0
        for jobId in jobList:
            jobRows = self.getNumberOfRowsById(jobId)
            try:
                rowSum += int(jobRows)
            except TypeError:
                # If jobRows is None or empty string, just don't add it, otherwise reraise
                if jobRows is None or jobRows == "":
                    continue
                else:
                    raise
        return rowSum

    def deleteSubmissionsForUserId(self,userId):
        """ Delete all submissions for a given user ID """
        self.session.query(Submission).filter(Submission.user_id == userId).delete()
        self.session.commit()

    def getFormattedDatetimeBySubmissionId(self, submissionId):
        """ Given a submission ID, return MM/DD/YYYY for the datetime of that submission """
        # TODO refactor datetime_utc to use one of the Date formats in postgres, change here and where it is created
        datetime = self.getSubmissionById(submissionId).datetime_utc
        return datetime.strftime("%m/%d/%Y")