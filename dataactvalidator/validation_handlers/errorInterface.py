from dataactcore.models.errorModels import ErrorMetadata
from dataactvalidator.validation_handlers.validationError import ValidationError

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.lookups import ERROR_TYPE_DICT


def record_row_error(error_list, job_id, filename, field_name, error_type, row, original_label=None, file_type_id=None,
                     target_file_id=None, severity_id=None):
    """ Add this error to running sum of error types

    Args:
        error_list: dict keeping track of error metadata to be updated
        job_id: ID of job in job tracker
        filename: name of error report in S3
        field_name: name of field where error occurred
        error_type: type of error, value will be mapped to ValidationError class,
            for rule failures this will hold entire message
        row: Row number error occurred on
        original_label: Label of rule
        file_type_id: Id of source file type
        target_file_id: Id of target file type
        severity_id: Id of error severity

    Returns:
        updated error_list with new/updated record rows
    """
    key = "".join([str(job_id), field_name, str(error_type)])
    if key in error_list:
        error_list[key]["numErrors"] += 1
    else:
        error_dict = {"filename": filename, "fieldName": field_name, "jobId": job_id, "errorType": error_type,
                      "numErrors": 1,
                      "firstRow": row, "originalRuleLabel": original_label, "fileTypeId": file_type_id,
                      "targetFileId": target_file_id, "severity": severity_id}
        error_list[key] = error_dict
    return error_list


def write_all_row_errors(error_list, job_id):
    """ Writes all recorded errors to database

    Args:
        error_list: dict keeping track of error metadata to be updated
        job_id: ID to write errors for
    """
    sess = GlobalDB.db().session
    for key in error_list.keys():
        error_dict = error_list[key]
        # Set info for this error
        this_job = error_dict["jobId"]
        if int(job_id) != int(this_job):
            # This row is for a different job, skip it
            continue
        field_name = error_dict["fieldName"]
        try:
            # If last part of key is an int, it's one of our prestored messages
            error_type = int(error_dict["errorType"])
        except ValueError:
            # For rule failures, it will hold the error message
            error_msg = error_dict["errorType"]
            if "Field must be no longer than specified limit" in error_msg:
                rule_failed_id = ERROR_TYPE_DICT['length_error']
            else:
                rule_failed_id = ERROR_TYPE_DICT['rule_failed']
            error_row = ErrorMetadata(job_id=this_job, filename=error_dict["filename"], field_name=field_name,
                                      error_type_id=rule_failed_id, rule_failed=error_msg,
                                      occurrences=error_dict["numErrors"], first_row=error_dict["firstRow"],
                                      original_rule_label=error_dict["originalRuleLabel"],
                                      file_type_id=error_dict["fileTypeId"],
                                      target_file_type_id=error_dict["targetFileId"],
                                      severity_id=error_dict["severity"])
        else:
            # This happens if cast to int was successful
            error_string = ValidationError.get_error_type_string(error_type)
            error_id = ERROR_TYPE_DICT[error_string]
            # Create error metadata
            error_row = ErrorMetadata(job_id=this_job, filename=error_dict["filename"], field_name=field_name,
                                      error_type_id=error_id, occurrences=error_dict["numErrors"],
                                      first_row=error_dict["firstRow"],
                                      rule_failed=ValidationError.get_error_message(error_type),
                                      original_rule_label=error_dict["originalRuleLabel"],
                                      file_type_id=error_dict["fileTypeId"],
                                      target_file_type_id=error_dict["targetFileId"],
                                      severity_id=error_dict["severity"])

        sess.add(error_row)
    # Commit the session to write all rows
    sess.commit()
