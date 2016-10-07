from datetime import datetime, date
from dataactcore.models.jobModels import Job,JobDependency,Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.errorHandler import ErrorHandler
from sqlalchemy import and_
import time

class JobHandler(JobTrackerInterface):
    """ Responsible for all interaction with the job tracker database
    Class fields:
    metaDataFieldMap -- Maps names in the request object to database column names

    Instance fields:
    engine -- sqlalchemy engine for generating connections and sessions
    connection -- sqlalchemy connection for executing direct SQL statements
    session -- sqlalchemy session for ORM usage
    """

    fiscalStartMonth = 10
    metaDataFieldMap = {"cgac_code":"cgac_code","reporting_period_start_date":"reporting_start_date","reporting_period_end_date":"reporting_end_date","is_quarter":"is_quarter_format"}

    def getSubmissionById(self,submissionId):
        """ Return submission object that matches ID """
        query = self.session.query(Submission).filter(Submission.submission_id == submissionId)
        result = self.runUniqueQuery(query,"No submission with that ID","Multiple submissions with that ID")
        return result

    def getSubmissionsByUserAgency(self,user,limit=5):
        """ Returns all submissions associated with the specified user's agency """
        return self.session.query(Submission).filter(Submission.cgac_code == user.cgac_code).order_by(Submission.updated_at.desc()).limit(limit).all()

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
                    reportDate = requestDict.getValue(key)

                    # Create a date object from formatted string, assuming "MM/YYYY"
                    try:
                        submissionData[cls.metaDataFieldMap[key]] = JobHandler.createDate(reportDate)
                    except ValueError as e:
                        # Bad value, must be MM/YYYY
                        raise ResponseException("Date must be provided as MM/YYYY",StatusCode.CLIENT_ERROR,ValueError)
                else:
                    submissionData[cls.metaDataFieldMap[key]] = requestDict.getValue(key)
            else:
                if not existingSubmission:
                    raise ResponseException(key + " is required",StatusCode.CLIENT_ERROR,ValueError)
        return submissionData, existingSubmissionId

    def getStartDate(self, submission):
        """ Return formatted start date """
        if submission.is_quarter_format:
            quarter = self.monthToQuarter(submission.reporting_start_date.month, True)
            year = submission.reporting_start_date.year
            if quarter == "Q1":
                # First quarter is part of next fiscal year
                year += 1
            return "".join([quarter,"/",str(year)])
        else:
            return submission.reporting_start_date.strftime("%m/%Y")

    def getEndDate(self, submission):
        """ Return formatted end date """
        if submission.is_quarter_format:
            quarter = self.monthToQuarter(submission.reporting_end_date.month, False)
            year = submission.reporting_end_date.year
            if quarter == "Q1":
                # First quarter is part of next fiscal year
                year += 1
            return "".join([quarter,"/",str(year)])
        else:
            return submission.reporting_end_date.strftime("%m/%Y")

    @classmethod
    def monthToQuarter(cls, month, isStart):
        """ Convert month as int to a two character quarter """
        # Base off fiscal year beginning
        baseMonth =  cls.fiscalStartMonth
        if not isStart:
            # Quarters end two months after they start
            baseMonth += 2
        monthsIntoFiscalYear = (month - baseMonth) % 12
        if (monthsIntoFiscalYear % 3) != 0:
            # Not a valid month for a quarter
            raise ResponseException("Not a valid month to be in quarter format", StatusCode.INTERNAL_ERROR, ValueError)
        quartersFromStart = monthsIntoFiscalYear / 3
        quarter = quartersFromStart + 1
        return "".join(["Q",str(int(quarter))])

    @staticmethod
    def quarterToMonth(quarter, isStart):
        """ Translate quarter as 'Q#' to a 2 digit month

        Args:
            quarter: Q followed by 1,2,3, or 4
            isStart: True if we want first month of quarter

        Returns:
            Two character string representing month
        """
        # If does not start with Q, this is an error
        if quarter[0] != "Q":
            raise ResponseException("Cannot translate quarter that does not begin with Q",StatusCode.CLIENT_ERROR,ValueError)

        # Specified by quarter, translate to months
        if quarter[1] == "1":
            if isStart:
                month = "10"
            else:
                month = "12"
        elif quarter[1] == "2":
            if isStart:
                month = "01"
            else:
                month = "03"
        elif quarter[1] == "3":
            if isStart:
                month = "04"
            else:
                month = "06"
        elif quarter[1] == "4":
            if isStart:
                month = "07"
            else:
                month = "09"
        else:
            raise ResponseException("Invalid quarter, must be 1-4",StatusCode.CLIENT_ERROR,ValueError)
        return month

    @staticmethod
    def createDate(dateString):
        """ Create a date object from a string in "MM/YYYY" """
        if dateString is None:
            return None
        dateParts = dateString.split("/")
        if len(dateParts) > 2:
            # Cannot include day now
            raise ResponseException("Please format dates as MM/YYYY",StatusCode.CLIENT_ERROR,ValueError)
        # Defaulting day to 1, this will not be used
        return date(year = int(dateParts[1]),month = int(dateParts[0]),day=1)

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
            self.setPublishStatus("unpublished", submission)
            self.session.add(submission)
        else:
            submissionQuery = self.session.query(Submission).filter(Submission.submission_id == existingId)
            submission = self.runUniqueQuery(submissionQuery,"No submission found with provided ID", "Multiple submissions found with provided ID")
            self.updatePublishStatus(submission)
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
            valQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.job_type_id == self.getJobTypeId("validation"))
            valJob = self.runUniqueQuery(valQuery,"No cross-file validation job found","Conflicting jobs found")
            valJob.job_status_id = self.getJobStatusId("waiting")
            extQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.job_type_id == self.getJobTypeId("external_validation"))
            extJob = self.runUniqueQuery(extQuery,"No external validation job found","Conflicting jobs found")
            extJob.job_status_id = self.getJobStatusId("waiting")

            # Update submission updated_at
            submission = self.getSubmissionById(submissionId)
            submission.updated_at = time.strftime("%c")
            self.session.commit()
        else:
            # Create validation job
            validationJob = Job(job_status_id=self.getJobStatusId("waiting"), job_type_id=self.getJobTypeId("validation"), submission_id=submissionId)
            self.session.add(validationJob)
            # Create external validation job
            externalJob = Job(job_status_id=self.getJobStatusId("waiting"), job_type_id=self.getJobTypeId("external_validation"), submission_id=submissionId)
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

    def addJobsForFileType(self, fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict):
        """ Add upload and validation jobs for a single filetype

        Args:
            fileType: What type of file to add jobs for
            filePath: Path to upload the file to
            filename: Original filename
            submissionId -- Submission ID to attach to jobs
            existingSubmission -- True if we should update existing jobs rather than creating new ones
            jobsRequired: List of job ids that will be prerequisites for cross-file job
            uploadDict: Dictionary of upload ids by filename to return to client, used for calling finalize_submission route

        Returns:
            jobsRequired: List of job ids that will be prerequisites for cross-file job
            uploadDict: Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """
        fileTypeQuery = self.session.query(FileType.file_type_id).filter(FileType.name == fileType)
        fileTypeResult = self.runUniqueQuery(fileTypeQuery,"No matching file type", "Multiple matching file types")
        fileTypeId = fileTypeResult.file_type_id

        if existingSubmission:
            # Find existing upload job and mark as running
            uploadQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.job_type_id == self.getJobTypeId("file_upload"))
            uploadJob = self.runUniqueQuery(uploadQuery,"No upload job found for this file","Conflicting jobs found")
            # Mark as running and set new file name and path
            uploadJob.job_status_id = self.getJobStatusId("running")
            uploadJob.original_filename = filename
            uploadJob.filename = filePath
            self.session.commit()
        else:
            if fileType in ["award","award_procurement"]:
                # File generation handled on backend, mark as ready
                uploadStatus = self.getJobStatusId("ready")
            elif fileType in ["awardee_attributes", "sub_award"]:
                # These are dependent on file D2 validation
                uploadStatus = self.getJobStatusId("waiting")
            else:
                # Mark as running since frontend should be doing this upload
                uploadStatus = self.getJobStatusId("running")
            uploadJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId, job_status_id=uploadStatus, job_type_id=self.getJobTypeId("file_upload"), submission_id=submissionId)
            self.session.add(uploadJob)
            self.session.commit()
        if existingSubmission:
            valQuery = self.session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.job_type_id == self.getJobTypeId("csv_record_validation"))
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
            if fileType == "awardee_attributes":
                if self.d1ValId is None:
                    raise Exception("Cannot create E job without a D1 job")
                # Add dependency on D1 validation job
                d1Dependency = JobDependency(job_id = uploadJob.job_id, prerequisite_id = self.d1ValId)
                self.session.add(d1Dependency)

            elif fileType == "sub_award":
                if self.cValId is None:
                    raise Exception("Cannot create F job without a C job")
                # Add dependency on C validation job
                d2Dependency = JobDependency(job_id = uploadJob.job_id, prerequisite_id = self.cValId)
                self.session.add(d2Dependency)
            else:
                # E and F don't get validation jobs
                valJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId, job_status_id=self.getJobStatusId("waiting"), job_type_id=self.getJobTypeId("csv_record_validation"), submission_id=submissionId)
                self.session.add(valJob)
                self.session.flush()
                # Add dependency between file upload and db upload
                uploadDependency = JobDependency(job_id = valJob.job_id, prerequisite_id = uploadJob.job_id)
                self.session.add(uploadDependency)
                self.session.commit()
                jobsRequired.append(valJob.job_id)

                if fileType == "award_financial":
                    # Record D2 val job ID
                    self.cValId = valJob.job_id
                elif fileType == "award_procurement":
                    self.d1ValId = valJob.job_id

            self.session.commit()

        uploadDict[fileType] = uploadJob.job_id
        return jobsRequired, uploadDict

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
        self.d1ValId = None
        self.cValId = None

        # First do award_financial and award_procurement jobs so they will be available for later dependencies
        for fileType, filePath, filename in filenames:
            if fileType in ["award_financial", "award_procurement"]:
                jobsRequired, uploadDict = self.addJobsForFileType(fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict)

        # Then do all other file types
        for fileType, filePath, filename in filenames:
            if fileType not in ["award_financial", "award_procurement"]:
                jobsRequired, uploadDict = self.addJobsForFileType(fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict)

        # Return list of upload jobs
        return jobsRequired, uploadDict

    def checkUploadType(self, jobId):
        """ Check that specified job is a file_upload job

        Args:
        jobId -- Job ID to check
        Returns:
        True if file upload, False otherwise
        """
        query = self.session.query(Job.job_type_id).filter(Job.job_id == jobId)
        result = self.checkJobUnique(query)
        # Got single job, check type
        if(result.job_type_id == self.getJobTypeId("file_upload")):
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

    def getFormattedDatetimeBySubmissionId(self, submissionId):
        """ Given a submission ID, return MM/DD/YYYY for the datetime of that submission """
        datetime = self.getSubmissionById(submissionId).datetime_utc
        return datetime.strftime("%m/%d/%Y")

    def getJobBySubmissionFileTypeAndJobType(self, submission_id, file_type_name, job_type_name):
        file_id = self.getFileTypeId(file_type_name)
        type_id = self.getJobTypeId(job_type_name)
        query = self.session.query(Job).filter(and_(Job.submission_id == submission_id, Job.file_type_id == file_id, Job.job_type_id == type_id))
        result = self.runUniqueQuery(query, "No job with that submission ID, file type and job type", "Multiple jobs with conflicting submission ID, file type and job type")
        return result

    def createFileTypeMap(self):
        """ Create a map from letter names to file type names """
        fileTypeMap = {}
        fileTypes = self.session.query(FileType).all()
        for fileType in fileTypes:
            fileTypeMap[fileType.letter_name] = fileType.name
        return fileTypeMap