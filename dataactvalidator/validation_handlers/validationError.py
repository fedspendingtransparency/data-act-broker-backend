class ValidationError:
    """ This class holds errors that can occur during validation, for use in the error report and database """

    typeErrorMsg = "The value provided was of the wrong type"
    typeError = 0
    requiredErrorMsg = "A required value was not provided"
    requiredError = 1
    valueErrorMsg = "The value provided was invalid"
    valueError = 2
    missingHeaderErrorMsg = "One of the required columns is not present in the file"
    missingHeaderError = 3
    badHeaderErrorMsg = "One of the headers in the file is not recognized"
    badHeaderError = 4
    readErrorMsg = "Could not parse this record correctly"
    readError = 5
    writeErrorMsg = "Could not write this record into the staging database"
    writeError = 6
    unknownErrorMsg = "An unknown error has occurred with this file"
    unknownError = 7
    singleRow = 8
    singleRowMsg = "CSV file must have a header row and at least one record"
    duplicateError = 9
    duplicateErrorMsg = "May not have the same header twice"
    jobError = 10
    jobErrorMsg = "Error occurred in job manager"
     # Create dict of error types
    errorDict = {typeError:typeErrorMsg, requiredError:requiredErrorMsg, valueError:valueErrorMsg, missingHeaderError:missingHeaderErrorMsg,
                 badHeaderError:badHeaderErrorMsg, readError:readErrorMsg, writeError:writeErrorMsg, unknownError:unknownErrorMsg,
                 singleRow:singleRowMsg,duplicateError:duplicateErrorMsg,jobError:jobErrorMsg}
    errorTypeDict = {typeError:"type_error",requiredError:"required_error",valueError:"value_error",missingHeaderError:"missing_header_error",
                     badHeaderError:"bad_header_error",readError:"read_error",writeError:"write_error",unknownError:"unknown_error",
                     singleRow:"single_row_error",duplicateError:"duplicate_header_error",jobError:"job_error"}

    @staticmethod
    def getErrorMessage(errorType):
        if(errorType in ValidationError.errorDict):
            return ValidationError.errorDict[errorType]
        else:
            # Not a valid error type
            raise ValueError("Called writeErrorMessage with an invalid error type")

    @staticmethod
    def getErrorTypeString(errorType):
        if(errorType in ValidationError.errorTypeDict):
            return ValidationError.errorTypeDict[errorType]
        else:
            # Not a valid error type
            raise ValueError("Called getErrorMessage with an invalid error type")