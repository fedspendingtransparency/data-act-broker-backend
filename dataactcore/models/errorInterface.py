from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import ErrorMetadata
from dataactvalidator.validation_handlers.validationError import ValidationError

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.lookups import ERROR_TYPE_DICT

class ErrorInterface(BaseInterface):
    """Manages communication with error database."""

    def __init__(self):
        """ Create empty row error dict """
        self.rowErrors = {}
        super(ErrorInterface, self).__init__()

    def recordRowError(self, job_id, filename, field_name, error_type, row, original_label=None, file_type_id=None,
                       target_file_id=None, severity_id=None):
        """ Add this error to running sum of error types

        Args:
            job_id: ID of job in job tracker
            filename: name of error report in S3
            field_name: name of field where error occurred
            error_type: type of error, value will be mapped to ValidationError class, for rule failures this will hold entire message
            row: Row number error occurred on
            original_label: Label of rule
            file_type_id: Id of source file type
            target_file_id: Id of target file type
            severity_id: Id of error severity
        """
        key = "".join([str(job_id), field_name, str(error_type)])
        if key in self.rowErrors:
            self.rowErrors[key]["numErrors"] += 1
        else:
            errorDict = {"filename": filename, "fieldName": field_name, "jobId": job_id, "errorType": error_type,
                         "numErrors": 1,
                         "firstRow": row, "originalRuleLabel": original_label, "fileTypeId": file_type_id,
                         "targetFileId": target_file_id, "severity": severity_id}
            self.rowErrors[key] = errorDict

    def writeAllRowErrors(self, job_id):
        """ Writes all recorded errors to database

        Args:
            job_id: ID to write errors for
        """
        sess = GlobalDB.db().session
        for key in self.rowErrors.keys():
            errorDict = self.rowErrors[key]
            # Set info for this error
            thisJob = errorDict["jobId"]
            if int(job_id) != int(thisJob):
                # This row is for a different job, skip it
                continue
            field_name = errorDict["fieldName"]
            try:
                # If last part of key is an int, it's one of our prestored messages
                error_type = int(errorDict["errorType"])
            except ValueError:
                # For rule failures, it will hold the error message
                errorMsg = errorDict["errorType"]
                if "Field must be no longer than specified limit" in errorMsg:
                    ruleFailedId = ERROR_TYPE_DICT['length_error']
                else:
                    ruleFailedId = ERROR_TYPE_DICT['rule_failed']
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=field_name,
                                         error_type_id=ruleFailedId, rule_failed=errorMsg,
                                         occurrences=errorDict["numErrors"], first_row=errorDict["firstRow"],
                                         original_rule_label=errorDict["originalRuleLabel"],
                                         file_type_id=errorDict["fileTypeId"],
                                         target_file_type_id=errorDict["targetFileId"],
                                         severity_id=errorDict["severity"])
            else:
                # This happens if cast to int was successful
                errorString = ValidationError.getErrorTypeString(error_type)
                errorId = ERROR_TYPE_DICT[errorString]
                # Create error metadata
                errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=field_name,
                                         error_type_id=errorId, occurrences=errorDict["numErrors"],
                                         first_row=errorDict["firstRow"],
                                         rule_failed=ValidationError.getErrorMessage(error_type),
                                         original_rule_label=errorDict["originalRuleLabel"],
                                         file_type_id=errorDict["fileTypeId"],
                                         target_file_type_id=errorDict["targetFileId"],
                                         severity_id=errorDict["severity"])

            sess.add(errorRow)

        # Commit the session to write all rows
        sess.commit()
        # Clear the dictionary
        self.rowErrors = {}