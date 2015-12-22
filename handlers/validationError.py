class ValidationError:
    """ This class holds an enum of errors that can occur during validation, for use in the error report and database """
    typeError = "The value provided was of the wrong type"
    requiredError = "A required value was not provided"
    valueError = "The value provided was invalid"
    missingHeaderError = "One of the required columns is not present in the file"
    badHeaderError = "One of the headers in the file is not recognized"

    @staticmethod
    def writeError(errorArray):
        """ Takes an array of strings and writes that row to S3

        Args:
        errorArray - Should be an array with three elements: field name, error description, and row number

        """
