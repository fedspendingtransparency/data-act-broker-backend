from datetime import datetime, date

from dataactcore.interfaces.function_bag import addJobsForFileType
from dataactcore.models.jobModels import Job,JobDependency,Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from sqlalchemy import and_
import time

from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT

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

    def createJobs(self, filenames, submission_id, existing_submission = False):
        """  Given the filenames to be uploaded, create the set of jobs needing to be completed for this submission

        Arguments:
        filenames -- List of tuples containing (file type, upload path, original filenames)
        submission_id -- Submission ID to be linked to jobs
        existing_submission -- True if we should update jobs in an existing submission rather than creating new jobs

        Returns:
        Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """


        jobs_required, upload_dict = self.addUploadJobs(filenames,submission_id,existing_submission)

        if existing_submission:
            # Find cross-file and external validation jobs and mark them as waiting
            val_query = self.session.query(Job).filter(Job.submission_id == submission_id).filter(Job.job_type_id == JOB_TYPE_DICT["validation"])
            val_job = self.runUniqueQuery(val_query,"No cross-file validation job found","Conflicting jobs found")
            val_job.job_status_id = JOB_STATUS_DICT["waiting"]
            ext_query = self.session.query(Job).filter(Job.submission_id == submission_id).filter(Job.job_type_id == JOB_TYPE_DICT["external_validation"])
            ext_job = self.runUniqueQuery(ext_query,"No external validation job found","Conflicting jobs found")
            ext_job.job_status_id = JOB_STATUS_DICT["waiting"]

            # Update submission updated_at
            submission = self.session.query(Submission).filter_by(submission_id = submission_id).one()
            submission.updated_at = time.strftime("%c")
            self.session.commit()
        else:
            # Create validation job
            validation_job = Job(job_status_id=JOB_STATUS_DICT["waiting"], job_type_id=JOB_TYPE_DICT["validation"], submission_id=submission_id)
            self.session.add(validation_job)
            # Create external validation job
            external_job = Job(job_status_id=JOB_STATUS_DICT["waiting"], job_type_id=JOB_TYPE_DICT["external_validation"], submission_id=submission_id)
            self.session.add(external_job)
            self.session.flush()
            # Create dependencies for validation jobs
            for job_id in jobs_required:
                val_dependency = JobDependency(job_id = validation_job.job_id, prerequisite_id = job_id)
                self.session.add(val_dependency)
                ext_dependency = JobDependency(job_id = external_job.job_id, prerequisite_id = job_id)
                self.session.add(ext_dependency)

        # Commit all changes
        self.session.commit()
        upload_dict["submission_id"] = submission_id
        return upload_dict

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

        # First do award_financial and award_procurement jobs so they will be available for later dependencies
        for fileType, filePath, filename in filenames:
            if fileType in ["award_financial", "award_procurement"]:
                jobsRequired, uploadDict = addJobsForFileType(fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict)

        # Then do all other file types
        for fileType, filePath, filename in filenames:
            if fileType not in ["award_financial", "award_procurement"]:
                jobsRequired, uploadDict = addJobsForFileType(fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict)

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
        if result.job_type_id == JOB_TYPE_DICT["file_upload"]:
            # Correct type
            return True
        # Did not confirm correct type
        return False

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

    def sumNumberOfRowsForJobList(self, jobs):
        """ Given a list of job IDs, return the number of rows summed across jobs """
        # temporary fix until jobHandler.py is refactored away
        jobList = [j.job_id for j in jobs]
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

    def getFormattedDatetimeBySubmissionId(self, submission_id):
        """ Given a submission ID, return MM/DD/YYYY for the datetime of that submission """
        datetime = self.session.query(Submission).filter_by(submission_id=submission_id).one().datetime_utc
        return datetime.strftime("%m/%d/%Y")

    def getJobBySubmissionFileTypeAndJobType(self, submission_id, file_type_name, job_type_name):
        file_id = FILE_TYPE_DICT[file_type_name]
        type_id = JOB_TYPE_DICT[job_type_name]
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
