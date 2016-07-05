from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
from dataactcore.utils.responseException import ResponseException
from dataactcore.models.errorModels import File, ErrorMetadata
from dataactcore.models.errorInterface import ErrorInterface
from dataactvalidator.validation_handlers.validationError import ValidationError

class ValidatorErrorInterface(ErrorInterface):
    """ Manages communication with the error database """

    def __init__(self):
        """ Create empty row error dict """
        self.rowErrors = {}
        super(ValidatorErrorInterface, self).__init__()

    def createFile(self, jobId, filename):
        """ Create a new file object for specified job and filename """
        try:
            int(jobId)
        except:
            raise ValueError("".join(["Bad jobId: ", str(jobId)]))

        fileRec = File(job_id=jobId,
                       filename=filename,
                       row_errors_present=False,
                       file_status_id=self.getFileStatusId("incomplete"))
        self.session.add(fileRec)
        self.session.commit()
        return fileRec

    def createFileIfNeeded(self, jobId, filename):
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

    def writeFileError(self, jobId, filename, errorType, extraInfo = None):
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

    def markFileComplete(self, jobId, filename):
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

    def recordRowError(self, jobId, filename, fieldName, errorType, row, original_label, file_type_id = None, target_file_id = None):
        """ Add this error to running sum of error types

        Args:
            jobId: ID of job in job tracker
            filename: name of error report in S3
            fieldName: name of field where error occurred
            errorType: type of error, value will be mapped to ValidationError class, for rule failures this will hold entire message

        Returns:
            True if successful
        """
        key = "".join([str(jobId),fieldName,str(errorType)])
        if(key in self.rowErrors):
            self.rowErrors[key]["numErrors"] += 1
        else:
            errorDict = {"filename":filename, "fieldName":fieldName, "jobId":jobId,"errorType":errorType,"numErrors":1, "firstRow":row, "originalRuleLabel":original_label, "fileTypeId": file_type_id, "targetFileId": target_file_id}
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
            if(int(jobId) != int(thisJob)):
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
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName, error_type_id=ruleFailedId, rule_failed=errorMsg, occurrences=errorDict["numErrors"], first_row=errorDict["firstRow"], original_rule_label=errorDict["originalRuleLabel"], file_type_id = errorDict["fileTypeId"], target_file_type_id = errorDict["targetFileId"])
            else:
                # This happens if cast to int was successful
                errorString = ValidationError.getErrorTypeString(errorType)
                errorId = self.getTypeId(errorString)
                # Create error metadata
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName, error_type_id=errorId, occurrences=errorDict["numErrors"], first_row=errorDict["firstRow"], rule_failed=ValidationError.getErrorMessage(errorType), original_rule_label=errorDict["originalRuleLabel"], file_type_id = errorDict["fileTypeId"], target_file_type_id = errorDict["targetFileId"])

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

    def setRowErrorsPresent(self, jobId, errorsPresent):
        """ Set errors present for the specified job ID to true or false.  Note this refers only to row-level errors, not file-level errors. """
        fileRec = self.getFileByJobId(jobId)
        # If errorsPresent is not a bool, this function will raise a TypeError
        fileRec.row_errors_present = bool(errorsPresent)
        self.session.commit()

    def getRowErrorsPresent(self, jobId):
        """ Returns True or False depending on if errors were found in the specified job """
        return self.getFileByJobId(jobId).row_errors_present
