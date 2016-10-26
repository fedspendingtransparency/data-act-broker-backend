from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import ErrorMetadata
from dataactvalidator.validation_handlers.validationError import ValidationError

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.lookups import FILE_STATUS_DICT, ERROR_TYPE_DICT

from dataactcore.interfaces.function_bag import createFileIfNeeded

class ErrorInterface(BaseInterface):
    """Manages communication with error database."""

    def __init__(self):
        """ Create empty row error dict """
        self.rowErrors = {}
        super(ErrorInterface, self).__init__()

    def writeFileError(self, jobId, filename, errorType, extraInfo=None):
        """ Write a file-level error to the file table

        Args:
            jobId: ID of job in job tracker
            filename: name of error report in S3
            errorType: type of error, value will be mapped to ValidationError class
            extraInfo: list of extra information to be included in file

        Returns:
            True if successful
        """
        sess = GlobalDB.db().session
        try:
            int(jobId)
        except:
            raise ValueError("".join(["Bad jobId: ", str(jobId)]))

        # Get File object for this job ID or create it if it doesn't exist
        fileRec = createFileIfNeeded(jobId, filename)

        # Mark error type and add header info if present
        fileRec.file_status_id = FILE_STATUS_DICT[ValidationError.getErrorTypeString(errorType)]
        if extraInfo is not None:
            if "missing_headers" in extraInfo:
                fileRec.headers_missing = extraInfo["missing_headers"]
            if "duplicated_headers" in extraInfo:
                fileRec.headers_duplicated = extraInfo["duplicated_headers"]

        sess.add(fileRec)
        sess.commit()
        return True

    def markFileComplete(self, job_id, filename=None):
        """ Marks file's status as complete

        Args:
            job_id: ID of job in job tracker
            filename: name of error report in S3

        Returns:
            True if successful
        """

        fileComplete = createFileIfNeeded(job_id, filename)
        fileComplete.file_status_id = FILE_STATUS_DICT['complete']
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
        if key in self.rowErrors:
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
            if int(jobId) != int(thisJob):
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
                    ruleFailedId = ERROR_TYPE_DICT['length_error']
                else:
                    ruleFailedId = ERROR_TYPE_DICT['rule_failed']
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
                errorId = ERROR_TYPE_DICT[errorString]
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