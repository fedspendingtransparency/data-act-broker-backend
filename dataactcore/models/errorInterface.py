from sqlalchemy.orm import joinedload
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import Status, ErrorType, FileStatus, ErrorData
from dataactcore.config import CONFIG_DB


class ErrorInterface(BaseInterface):
    """Manages communication with error database."""
    dbConfig = CONFIG_DB
    dbName = dbConfig['error_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['error_db_name']
        super(ErrorInterface, self).__init__()

    @staticmethod
    def getDbName():
        """Return database name."""
        return ErrorInterface.dbName

    def getSession(self):
        return self.session

    def getStatusId(self,statusName):
        """Get status ID for given name."""
        return self.getIdFromDict(
            Status, "STATUS_DICT", "name", statusName, "status_id")

    def getTypeId(self,typeName):
        return self.getIdFromDict(
            ErrorType, "TYPE_DICT", "name", typeName, "error_type_id")

    def getFileStatusByJobId(self, jobId):
        """ Get the File Status object with the specified job ID

        Args:
            jobId: job to get file status for

        Returns:
            A File Status model object
        """
        query = self.session.query(FileStatus).filter(FileStatus.job_id == jobId)
        return self.runUniqueQuery(query,"No file for that job ID", "Multiple files have been associated with that job ID")

    def checkStatusByJobId(self, jobId):
        """ Query status for specified job

        Args:
            jobId: job to check status for

        Returns:
            Status ID of specified job
        """
        return self.getFileStatusByJobId(jobId).status_id

    def getStatusLabelByJobId(self, jobId):
        """ Query status label for specified job

        Args:
            jobId: job to check status for

        Returns:
            Status label of specified job (string)
        """
        query = self.session.query(FileStatus).options(joinedload("status")).filter(FileStatus.job_id == jobId)
        return self.runUniqueQuery(query,"No file for that job ID", "Multiple files have been associated with that job ID").status.name

    def checkNumberOfErrorsByJobId(self, jobId):
        """ Get the total number of errors for a specified job

        Args:
            jobId: job to get errors for

        Returns:
            Number of errors for specified job
        """
        queryResult = self.session.query(ErrorData).filter(ErrorData.job_id == jobId).all()
        numErrors = 0
        for result in queryResult:
            # For each row that matches jobId, add the number of that type of error
            numErrors += result.occurrences
        return numErrors

    def resetErrorsByJobId(self, jobId):
        """ Clear all entries in ErrorData for a specified job

        Args:
            jobId: job to reset
        """
        self.session.query(ErrorData).filter(ErrorData.job_id == jobId).delete()
        self.session.commit()

    def sumNumberOfErrorsForJobList(self,jobIdList):
        """ Add number of errors for all jobs in list """
        errorSum = 0
        for jobId in jobIdList:
            jobErrors = self.checkNumberOfErrorsByJobId(jobId)
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
        return self.getFileStatusByJobId(jobId).headers_missing

    def getDuplicatedHeadersByJobId(self, jobId):
        return self.getFileStatusByJobId(jobId).headers_duplicated

    def getErrorType(self,jobId):
        """ Returns either "none", "header_errors", or "row_errors" depending on what errors occurred during validation """
        if self.getStatusLabelByJobId(jobId) == "header_error":
            # Header errors occurred, return that
            return "header_errors"
        elif self.getFileStatusByJobId(jobId).row_errors_present:
            # Row errors occurred
            return "row_errors"
        else:
            # No errors occurred during validation
            return "none"
