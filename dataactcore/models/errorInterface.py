from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import FileStatus, ErrorType, File, ErrorMetadata



class ErrorInterface(BaseInterface):
    """Manages communication with error database."""

    def __init__(self):
        super(ErrorInterface, self).__init__()

    def getFileStatusId(self,statusName):
        """Get file status ID for given name."""
        return self.getIdFromDict(
            FileStatus, "FILE_STATUS_DICT", "name", statusName, "file_status_id")

    def getTypeId(self,typeName):
        """Get type ID for given name """
        return self.getIdFromDict(
            ErrorType, "TYPE_DICT", "name", typeName, "error_type_id")

    def getFileByJobId(self, jobId):
        """ Get the File object with the specified job ID

        Args:
            jobId: job to get file for

        Returns:
            A File model object
        """
        query = self.session.query(File).filter(File.job_id == jobId)
        return self.runUniqueQuery(query,"No file for that job ID", "Multiple files have been associated with that job ID")

    def checkFileStatusByJobId(self, jobId):
        """ Query file status for specified job

        Args:
            jobId: job to check status for

        Returns:
            File Status ID of specified job
        """
        return self.getFileByJobId(jobId).file_status_id

    def getFileStatusLabelByJobId(self, jobId):
        """ Query file status label for specified job

        Args:
            jobId: job to check status for

        Returns:
            File status label (aka name) for specified job (string)
        """
        query = self.session.query(File).options(joinedload("file_status")).filter(File.job_id == jobId)
        return self.runUniqueQuery(query,"No file for that job ID", "Multiple files have been associated with that job ID").file_status.name

    def checkNumberOfErrorsByJobId(self, jobId, valDb, errorType = "fatal"):
        """ Get the total number of errors for a specified job

        Args:
            jobId: job to get errors for

        Returns:
            Number of errors for specified job
        """
        queryResult = self.session.query(ErrorMetadata).filter(ErrorMetadata.job_id == jobId).all()
        numErrors = 0
        for result in queryResult:
            if result.severity_id != valDb.getRuleSeverityId(errorType):
                # Don't count other error types
                continue
            # For each row that matches jobId, add the number of that type of error
            numErrors += result.occurrences
        return numErrors

    def resetErrorsByJobId(self, jobId):
        """ Clear all entries in ErrorMetadata for a specified job

        Args:
            jobId: job to reset
        """
        self.session.query(ErrorMetadata).filter(ErrorMetadata.job_id == jobId).delete()
        self.session.commit()

    def sumNumberOfErrorsForJobList(self,jobIdList, valDb, errorType = "fatal"):
        """ Add number of errors for all jobs in list """
        errorSum = 0
        for jobId in jobIdList:
            jobErrors = self.checkNumberOfErrorsByJobId(jobId, valDb, errorType)
            self.interfaces.jobDb.setJobNumberOfErrors(jobId, jobErrors, errorType)
            try:
                errorSum += int(jobErrors)
            except TypeError:
                # If jobRows is None or empty string, just don't add it, otherwise reraise
                if jobErrors is None or jobErrors == "":
                    continue
                else:
                    raise
        return errorSum

    def getMissingHeadersByJobId(self, jobId):
        """ Get a comma delimited string of all missing headers for specified job """
        return self.getFileByJobId(jobId).headers_missing

    def getDuplicatedHeadersByJobId(self, jobId):
        """ Get a comma delimited string of all duplicated headers for specified job """
        return self.getFileByJobId(jobId).headers_duplicated

    def getErrorType(self,jobId):
        """ Returns either "none", "header_errors", or "row_errors" depending on what errors occurred during validation """
        if self.getFileStatusLabelByJobId(jobId) == "header_error":
            # Header errors occurred, return that
            return "header_errors"
        elif self.getFileByJobId(jobId).row_errors_present:
            # Row errors occurred
            return "row_errors"
        else:
            # No errors occurred during validation
            return "none"

    def resetFileByJobId(self, jobId):
        """ Delete file for job ID """
        self.session.query(File).filter(File.job_id == jobId).delete()
        self.session.commit()

    def getCrossReportName(self, submissionId, sourceFile, targetFile):
        """ Create error report filename based on source and target file """
        return "submission_{}_cross_{}_{}.csv".format(submissionId, sourceFile, targetFile)

    def getCrossWarningReportName(self, submissionId, sourceFile, targetFile):
        """ Create error report filename based on source and target file """
        return "submission_{}_cross_warning_{}_{}.csv".format(submissionId, sourceFile, targetFile)