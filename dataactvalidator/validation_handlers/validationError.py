class ValidationError:
    """ This class holds errors that can occur during validation, for use in the error report and database """

    typeErrorMsg = "The value provided was of the wrong type"
    typeError = 0
    requiredErrorMsg = "A required value was not provided"
    requiredError = 1
    valueErrorMsg = "The value provided was invalid"
    valueError = 2
    headerErrorMsg = "The file has errors in the header row"
    headerError = 3
    readErrorMsg = "Could not parse this record correctly"
    readError = 4
    writeErrorMsg = "Could not write this record into the staging table"
    writeError = 5
    unknownErrorMsg = "An unknown error has occurred with this file"
    unknownError = 6
    singleRow = 7
    singleRowMsg = "CSV file must have a header row and at least one record"
    jobError = 8
    jobErrorMsg = "Error occurred in job manager"
    lengthError = 9
    lengthErrorMsg = "Value was longer than maximum length for this field"
     # Create dict of error types
    errorDict = {typeError:typeErrorMsg, requiredError:requiredErrorMsg, valueError:valueErrorMsg, headerError:headerErrorMsg,
                 readError:readErrorMsg, writeError:writeErrorMsg, unknownError:unknownErrorMsg,
                 singleRow:singleRowMsg,jobError:jobErrorMsg, lengthError:lengthErrorMsg}
    errorTypeDict = {typeError:"type_error",requiredError:"required_error",valueError:"value_error",headerError:"header_error",
                     readError:"read_error",writeError:"write_error",unknownError:"unknown_error",
                     singleRow:"single_row_error",jobError:"job_error",lengthError:"length_error"}

    @staticmethod
    def getErrorMessage(errorType):
        """ Retrieve error message for specified error type """
        if errorType == None:
            # If no error type is provided, this is an unknown error
            errorType = ValidationError.unknownError
        if(errorType in ValidationError.errorDict):
            return ValidationError.errorDict[errorType]
        else:
            # Not a valid error type
            raise ValueError("Called getErrorMessage with an invalid error type: " + str(errorType))

    @staticmethod
    def getErrorTypeString(errorType):
        """ Get string identifier used in database for specified error type """
        if errorType == None:
            # If no error type is provided, this is an unknown error
            errorType = ValidationError.unknownError
        if(errorType in ValidationError.errorTypeDict):
            return ValidationError.errorTypeDict[errorType]
        else:
            # Not a valid error type
            raise ValueError("Called getErrorTypeString with an invalid error type: " + str(errorType))