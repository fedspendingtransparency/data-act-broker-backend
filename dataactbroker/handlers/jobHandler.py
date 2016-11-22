from datetime import date

from dataactcore.models.jobModels import Job, Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from sqlalchemy import and_

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.lookups import JOB_TYPE_DICT, FILE_TYPE_DICT

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
        sess = GlobalDB.db().session
        # temporary fix until jobHandler.py is refactored away
        jobList = [j.job_id for j in jobs]
        rowSum = 0
        for jobId in jobList:
            jobRows = sess.query(Job).filter_by(job_id = jobId).one().number_of_rows
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
