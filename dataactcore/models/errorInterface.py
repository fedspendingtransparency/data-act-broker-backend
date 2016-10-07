from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import FileStatus, ErrorType, File, ErrorMetadata
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError


class ErrorInterface(BaseInterface):
    """Manages communication with error database."""

    def __init__(self):
        """ Create empty row error dict """
        self.rowErrors = {}
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
        """Deprecated: moved to function_bag.py."""
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
        """Deprecated: moved to function_bag.py."""
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
        elif self.interfaces.jobDb.getJobById(jobId).number_of_errors > 0:
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

    def createFileIfNeeded(self, jobId, filename = None):
        """ Return the existing file object if it exists, or create a new one """
        try:
            fileRec = self.getFileByJobId(jobId)
            # Set new filename for changes to an existing submission
            fileRec.filename = filename
        except ResponseException as e:
            if isinstance(e.wrappedException, NoResultFound):
                # No File object for this job ID, just create one
                fileRec = self.createFile(jobId, filename)
            else:
                # Other error types should be handled at a higher level, so re-raise
                raise
        return fileRec

    def createFile(self, jobId, filename):
        """ Create a new file object for specified job and filename """
        try:
            int(jobId)
        except:
            raise ValueError("".join(["Bad jobId: ", str(jobId)]))

        fileRec = File(job_id=jobId,
                       filename=filename,
                       file_status_id=self.getFileStatusId("incomplete"))
        self.session.add(fileRec)
        self.session.commit()
        return fileRec

    def writeFileError(self, jobId, filename, errorType, extraInfo=None):
        """ Write a file-level error to the file table

        Args:
            jobId: ID of job in job tracker
            filename: name of error report in S3
            errorType: type of error, value will be mapped to ValidationError class

        Returns:
            True if successful
        """
        try:
            int(jobId)
        except:
            raise ValueError("".join(["Bad jobId: ", str(jobId)]))

        # Get File object for this job ID or create it if it doesn't exist
        fileRec = self.createFileIfNeeded(jobId, filename)

        # Mark error type and add header info if present
        fileRec.file_status_id = self.getFileStatusId(
            ValidationError.getErrorTypeString(errorType))
        if extraInfo is not None:
            if "missing_headers" in extraInfo:
                fileRec.headers_missing = extraInfo["missing_headers"]
            if "duplicated_headers" in extraInfo:
                fileRec.headers_duplicated = extraInfo["duplicated_headers"]

        self.session.add(fileRec)
        self.session.commit()
        return True

    def markFileComplete(self, jobId, filename=None):
        """ Marks file's status as complete

        Args:
            jobId: ID of job in job tracker
            filename: name of error report in S3

        Returns:
            True if successful
        """

        fileComplete = self.createFileIfNeeded(jobId, filename)
        fileComplete.file_status_id = self.getFileStatusId("complete")
        self.session.commit()
        return True

    def recordRowError(self, jobId, filename, fieldName, errorType, row, original_label=None, file_type_id=None,
                       target_file_id=None, severity_id=None):
        """ Add this error to running sum of error types

        Args:
            jobId: ID of job in job tracker
            filename: name of error report in S3
            fieldName: name of field where error occurred
            errorType: type of error, value will be mapped to ValidationError class, for rule failures this will hold entire message
            row: Row number error occurred on
            original_label: Label of rule
            file_type_id: Id of source file type
            target_file_id: Id of target file type
            severity_id: Id of error severity
        Returns:
            True if successful
        """
        key = "".join([str(jobId), fieldName, str(errorType)])
        if (key in self.rowErrors):
            self.rowErrors[key]["numErrors"] += 1
        else:
            errorDict = {"filename": filename, "fieldName": fieldName, "jobId": jobId, "errorType": errorType,
                         "numErrors": 1,
                         "firstRow": row, "originalRuleLabel": original_label, "fileTypeId": file_type_id,
                         "targetFileId": target_file_id, "severity": severity_id}
            self.rowErrors[key] = errorDict

    def writeAllRowErrors(self, jobId):
        """ Writes all recorded errors to database

        Args:
            jobId: ID to write errors for

        Returns:
            True if successful
        """
        for key in self.rowErrors.keys():
            errorDict = self.rowErrors[key]
            # Set info for this error
            thisJob = errorDict["jobId"]
            if (int(jobId) != int(thisJob)):
                # This row is for a different job, skip it
                continue
            fieldName = errorDict["fieldName"]
            try:
                # If last part of key is an int, it's one of our prestored messages
                errorType = int(errorDict["errorType"])
            except ValueError:
                # For rule failures, it will hold the error message
                errorMsg = errorDict["errorType"]
                if "Field must be no longer than specified limit" in errorMsg:
                    ruleFailedId = self.getTypeId("length_error")
                else:
                    ruleFailedId = self.getTypeId("rule_failed")
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName,
                                         error_type_id=ruleFailedId, rule_failed=errorMsg,
                                         occurrences=errorDict["numErrors"], first_row=errorDict["firstRow"],
                                         original_rule_label=errorDict["originalRuleLabel"],
                                         file_type_id=errorDict["fileTypeId"],
                                         target_file_type_id=errorDict["targetFileId"],
                                         severity_id=errorDict["severity"])
            else:
                # This happens if cast to int was successful
                errorString = ValidationError.getErrorTypeString(errorType)
                errorId = self.getTypeId(errorString)
                # Create error metadata
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName,
                                         error_type_id=errorId, occurrences=errorDict["numErrors"],
                                         first_row=errorDict["firstRow"],
                                         rule_failed=ValidationError.getErrorMessage(errorType),
                                         original_rule_label=errorDict["originalRuleLabel"],
                                         file_type_id=errorDict["fileTypeId"],
                                         target_file_type_id=errorDict["targetFileId"],
                                         severity_id=errorDict["severity"])

            self.session.add(errorRow)

        # Commit the session to write all rows
        self.session.commit()
        # Clear the dictionary
        self.rowErrors = {}

    def writeMissingHeaders(self, jobId, missingHeaders):
        """ Write list of missing headers into headers_missing field

        Args:
            jobId: Job to write error for
            missingHeaders: List of missing headers

        """
        fileRec = self.getFileByJobId(jobId)
        # Create single string out of missing header list
        fileRec.headers_missing = ",".join(missingHeaders)
        self.session.commit()

    def writeDuplicatedHeaders(self, jobId, duplicatedHeaders):
        """ Write list of duplicated headers into headers_missing field

        Args:
            jobId: Job to write error for
            duplicatedHeaders: List of duplicated headers

        """
        fileRec = self.getFileByJobId(jobId)
        # Create single string out of duplicated header list
        fileRec.headers_duplicated = ",".join(duplicatedHeaders)
        self.session.commit()