from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.errorModels import FileStatus, ErrorType, File, ErrorMetadata
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError

database = GlobalDB.db()
sess = database.session

# Common stuff from old baseInterface

def checkUnique(queryResult, noResultMessage, multipleResultMessage):
    """ Check that result is unique, if not raise exception"""
    if (len(queryResult) == 0):
        # Did not get a result for this job, mark as a job error
        raise ResponseException(noResultMessage, StatusCode.CLIENT_ERROR, NoResultFound)

    elif (len(queryResult) > 1):
        # Multiple results for single job ID
        raise ResponseException(multipleResultMessage, StatusCode.INTERNAL_ERROR, MultipleResultsFound)

    return True


def runUniqueQuery(query, noResultMessage, multipleResultMessage):
    """ Run query looking for one result, if it fails wrap it in a ResponseException with an appropriate message """
    try:
        return query.one()
    except NoResultFound as e:
        if (noResultMessage == False):
            # Raise the exception as is, used for specific handling
            raise e
        raise ResponseException(noResultMessage, StatusCode.CLIENT_ERROR, NoResultFound)
    except MultipleResultsFound as e:
        raise ResponseException(multipleResultMessage, StatusCode.INTERNAL_ERROR, MultipleResultsFound)


def getIdFromDict(model, dictName, fieldName, fieldValue, idField):
    """ Populate a static dictionary to hold an id to name dictionary for specified model

    Args:
        model - Model to populate dictionary for
        dictName - Name of dictionary to be populated
        fieldName - Field that will be used to populate keys of dictionary
        fieldValue - Value being queried for (None to just set up dict without returning)
        idField - Field that will be used to populate values of dictionary

    Returns:
        Value in idField that corresponds to specified fieldValue in fieldName
    """
    dict = getattr(model, dictName)
    if (dict == None):
        dict = {}
        # Pull status values out of DB
        # Create new session for this
        queryResult = sess.query(model).all()
        for result in queryResult:
            dict[getattr(result, fieldName)] = getattr(result, idField)
        setattr(model, dictName, dict)
    if fieldValue is None:
        # Not looking for a return, just called to set up dict
        return None
    if (not fieldValue in dict):
        raise ValueError("Not a valid " + str(model) + ": " + str(fieldValue) + ", not found in dict: " + str(dict))
    return dict[fieldValue]


def getNameFromDict(model, dictName, fieldName, fieldValue, idField):
    """ This uses the dict attached to model backwards, to get the name from the ID.  This is slow and should not
    be used too widely """
    # Populate dict
    getIdFromDict(model, dictName, fieldName, None, idField)
    # Step through dict to find fieldValue
    dict = model.__dict__[dictName]
    for key in dict:
        if dict[key] == fieldValue:
            return key
    # If not found, raise an exception
    raise ValueError("Value: " + str(fieldValue) + " not found in dict: " + str(dict))


# From old error interfaces

def getFileStatusId(statusName):
    """Get file status ID for given name."""
    return getIdFromDict(
        FileStatus, "FILE_STATUS_DICT", "name", statusName, "file_status_id")


def getTypeId(typeName):
    """Get type ID for given name """
    return getIdFromDict(
        ErrorType, "TYPE_DICT", "name", typeName, "error_type_id")


def getFileByJobId(jobId):
    """ Get the File object with the specified job ID

    Args:
        jobId: job to get file for

    Returns:
        A File model object
    """
    query = sess.query(File).filter(File.job_id == jobId)
    return runUniqueQuery(query, "No file for that job ID", "Multiple files have been associated with that job ID")


def checkFileStatusByJobId(jobId):
    """ Query file status for specified job

    Args:
        jobId: job to check status for

    Returns:
        File Status ID of specified job
    """
    return getFileByJobId(jobId).file_status_id


def getFileStatusLabelByJobId(jobId):
    """ Query file status label for specified job

    Args:
        jobId: job to check status for

    Returns:
        File status label (aka name) for specified job (string)
    """
    query = sess.query(File).options(joinedload("file_status")).filter(File.job_id == jobId)
    return runUniqueQuery(query, "No file for that job ID",
                               "Multiple files have been associated with that job ID").file_status.name


def checkNumberOfErrorsByJobId(jobId, valDb, errorType="fatal"):
    """ Get the total number of errors for a specified job

    Args:
        jobId: job to get errors for

    Returns:
        Number of errors for specified job
    """
    queryResult = sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == jobId).all()
    numErrors = 0
    for result in queryResult:
        if result.severity_id != valDb.getRuleSeverityId(errorType):
            # Don't count other error types
            continue
        # For each row that matches jobId, add the number of that type of error
        numErrors += result.occurrences
    return numErrors


def resetErrorsByJobId(jobId):
    """ Clear all entries in ErrorMetadata for a specified job

    Args:
        jobId: job to reset
    """
    sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == jobId).delete()
    sess.commit()


def sumNumberOfErrorsForJobList(jobIdList, valDb, errorType="fatal"):
    """ Add number of errors for all jobs in list """
    errorSum = 0
    for jobId in jobIdList:
        jobErrors = checkNumberOfErrorsByJobId(jobId, valDb, errorType)
        interfaces.jobDb.setJobNumberOfErrors(jobId, jobErrors, errorType)
        try:
            errorSum += int(jobErrors)
        except TypeError:
            # If jobRows is None or empty string, just don't add it, otherwise reraise
            if jobErrors is None or jobErrors == "":
                continue
            else:
                raise
    return errorSum


def getMissingHeadersByJobId(jobId):
    """ Get a comma delimited string of all missing headers for specified job """
    return getFileByJobId(jobId).headers_missing


def getDuplicatedHeadersByJobId(jobId):
    """ Get a comma delimited string of all duplicated headers for specified job """
    return getFileByJobId(jobId).headers_duplicated


def getErrorType(jobId):
    """ Returns either "none", "header_errors", or "row_errors" depending on what errors occurred during validation """
    if getFileStatusLabelByJobId(jobId) == "header_error":
        # Header errors occurred, return that
        return "header_errors"
    elif getFileByJobId(jobId).row_errors_present:
        # Row errors occurred
        return "row_errors"
    else:
        # No errors occurred during validation
        return "none"


def resetFileByJobId(jobId):
    """ Delete file for job ID """
    sess.query(File).filter(File.job_id == jobId).delete()
    sess.commit()


def getCrossReportName(submissionId, sourceFile, targetFile):
    """ Create error report filename based on source and target file """
    return "submission_{}_cross_{}_{}.csv".format(submissionId, sourceFile, targetFile)


def getCrossWarningReportName(submissionId, sourceFile, targetFile):
    """ Create error report filename based on source and target file """
    return "submission_{}_cross_warning_{}_{}.csv".format(submissionId, sourceFile, targetFile)


def createFile(jobId, filename):
    """ Create a new file object for specified job and filename """
    try:
        int(jobId)
    except:
        raise ValueError("".join(["Bad jobId: ", str(jobId)]))

    fileRec = File(job_id=jobId,
                   filename=filename,
                   row_errors_present=False,
                   file_status_id=getFileStatusId("incomplete"))
    sess.add(fileRec)
    sess.commit()
    return fileRec


def createFileIfNeeded(jobId, filename):
    """ Return the existing file object if it exists, or create a new one """
    try:
        fileRec = getFileByJobId(jobId)
        # Set new filename for changes to an existing submission
        fileRec.filename = filename
    except ResponseException as e:
        if isinstance(e.wrappedException, NoResultFound):
            # No File object for this job ID, just create one
            fileRec = createFile(jobId, filename)
        else:
            # Other error types should be handled at a higher level, so re-raise
            raise
    return fileRec


def writeFileError(jobId, filename, errorType, extraInfo=None):
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
    fileRec = createFileIfNeeded(jobId, filename)

    # Mark error type and add header info if present
    fileRec.file_status_id = getFileStatusId(
        ValidationError.getErrorTypeString(errorType))
    if extraInfo is not None:
        if "missing_headers" in extraInfo:
            fileRec.headers_missing = extraInfo["missing_headers"]
        if "duplicated_headers" in extraInfo:
            fileRec.headers_duplicated = extraInfo["duplicated_headers"]

    sess.add(fileRec)
    sess.commit()
    return True


def markFileComplete(jobId, filename):
    """ Marks file's status as complete

    Args:
        jobId: ID of job in job tracker
        filename: name of error report in S3

    Returns:
        True if successful
    """

    fileComplete = createFileIfNeeded(jobId, filename)
    fileComplete.file_status_id = getFileStatusId("complete")
    sess.commit()
    return True


def recordRowError(jobId, filename, fieldName, errorType, row, original_label=None, file_type_id=None,
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
    if (key in rowErrors):
        rowErrors[key]["numErrors"] += 1
    else:
        errorDict = {"filename": filename, "fieldName": fieldName, "jobId": jobId, "errorType": errorType,
                     "numErrors": 1,
                     "firstRow": row, "originalRuleLabel": original_label, "fileTypeId": file_type_id,
                     "targetFileId": target_file_id, "severity": severity_id}
        rowErrors[key] = errorDict


def writeAllRowErrors(jobId):
    """ Writes all recorded errors to database

    Args:
        jobId: ID to write errors for

    Returns:
        True if successful
    """
    for key in rowErrors.keys():
        errorDict = rowErrors[key]
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
                ruleFailedId = getTypeId("length_error")
            else:
                ruleFailedId = getTypeId("rule_failed")
            errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName,
                                     error_type_id=ruleFailedId, rule_failed=errorMsg,
                                     occurrences=errorDict["numErrors"], first_row=errorDict["firstRow"],
                                     original_rule_label=errorDict["originalRuleLabel"],
                                     file_type_id=errorDict["fileTypeId"],
                                     target_file_type_id=errorDict["targetFileId"], severity_id=errorDict["severity"])
        else:
            # This happens if cast to int was successful
            errorString = ValidationError.getErrorTypeString(errorType)
            errorId = getTypeId(errorString)
            # Create error metadata
            errorRow = ErrorMetadata(job_id=thisJob, filename=errorDict["filename"], field_name=fieldName,
                                     error_type_id=errorId, occurrences=errorDict["numErrors"],
                                     first_row=errorDict["firstRow"],
                                     rule_failed=ValidationError.getErrorMessage(errorType),
                                     original_rule_label=errorDict["originalRuleLabel"],
                                     file_type_id=errorDict["fileTypeId"],
                                     target_file_type_id=errorDict["targetFileId"], severity_id=errorDict["severity"])

        sess.add(errorRow)

    # Commit the session to write all rows
    sess.commit()
    # Clear the dictionary
    rowErrors = {}


def writeMissingHeaders(jobId, missingHeaders):
    """ Write list of missing headers into headers_missing field

    Args:
        jobId: Job to write error for
        missingHeaders: List of missing headers

    """
    fileRec = getFileByJobId(jobId)
    # Create single string out of missing header list
    fileRec.headers_missing = ",".join(missingHeaders)
    sess.commit()


def writeDuplicatedHeaders(jobId, duplicatedHeaders):
    """ Write list of duplicated headers into headers_missing field

    Args:
        jobId: Job to write error for
        duplicatedHeaders: List of duplicated headers

    """
    fileRec = getFileByJobId(jobId)
    # Create single string out of duplicated header list
    fileRec.headers_duplicated = ",".join(duplicatedHeaders)
    sess.commit()


def setRowErrorsPresent(jobId, errorsPresent):
    """ Set errors present for the specified job ID to true or false.  Note this refers only to row-level errors, not file-level errors. """
    fileRec = getFileByJobId(jobId)
    # If errorsPresent is not a bool, this function will raise a TypeError
    fileRec.row_errors_present = bool(errorsPresent)
    sess.commit()


def getRowErrorsPresent(jobId):
    """ Returns True or False depending on if errors were found in the specified job """
    return getFileByJobId(jobId).row_errors_present
