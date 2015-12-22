from filestreaming.csvWriter import CsvWriter

class ValidationError:
    """ This class holds an enum of errors that can occur during validation, for use in the error report and database """
    typeError = "The value provided was of the wrong type"
    requiredError = "A required value was not provided"
    valueError = "The value provided was invalid"
    missingHeaderError = "One of the required columns is not present in the file"
    badHeaderError = "One of the headers in the file is not recognized"
    readError = "Could not parse this record correctly"
    writeError = "Could not write this record into the staging database"