from decimal import *
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.utils.cloudLogger import CloudLogger

class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE","FALSE","YES","NO","1","0"]
    tableAbbreviations = {"appropriations":"approp","award_financial_assistance":"afa","award_financial":"af","object_class_program_activity":"op","appropriation":"approp"}
    # Set of metadata fields that should not be directly validated
    META_FIELDS = ["row_number", "is_first_quarter"]

    @classmethod
    def crossValidateSql(cls, rules, submissionId):
        """ Evaluate all sql-based rules for cross file validation

        Args:
            rules -- List of Rule objects
            submissionId -- ID of submission to run cross-file validation
        """
        failures = []
        # Put each rule through evaluate, appending all failures into list
        interfaces = InterfaceHolder()
        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        for rule in rules:
            failedRows = interfaces.validationDb.connection.execute(
                rule.rule_sql.format(submissionId))
            if failedRows.rowcount:
                # get list of fields involved in this validation
                # note: row_number is metadata, not a field being
                # validated, so exclude it
                cols = failedRows.keys()
                cols.remove('row_number')
                columnString = ", ".join(shortColnames[c] if c in shortColnames else c for c in cols)
                for row in failedRows:
                    # get list of values for each column
                    values = ["{}: {}".format(shortColnames[c], str(row[c])) if c in shortColnames else "{}: {}".format(c, str(row[c])) for c in cols]
                    values = ", ".join(values)
                    targetFileType = interfaces.validationDb.getFileTypeById(rule.target_file_id)
                    failures.append([rule.file.name, targetFileType, columnString,
                        str(rule.rule_description), values, row['row_number'],str(rule.rule_label),rule.file_id,rule.target_file_id])

        # Return list of cross file validation failures
        return failures

    @classmethod
    def validate(cls,record,csvSchema,fileType,interfaces):
        """
        Args:
        record -- dict representation of a single record of data
        csvSchema -- dict of schema for the current file.
        fileType -- name of file type to check against

        Returns:
        Tuple of three values:
        True if validation passed, False if failed
        List of failed rules, each with field, description of failure, and value that failed
        True if type check passed, False if type failed
        """
        recordFailed = False
        recordTypeFailure = False
        failedRules = []

        for fieldName in csvSchema:
            if (csvSchema[fieldName].required and not fieldName in record):
                return False, [[fieldName, ValidationError.requiredError, "", ""]], False

        for fieldName in record :
            if fieldName in cls.META_FIELDS:
                # Skip fields that are not user submitted
                continue
            checkRequiredOnly = False
            currentSchema = csvSchema[fieldName]

            currentData = record[fieldName]
            if(currentData != None):
                currentData = currentData.strip()

            if(currentData == None or len(currentData) == 0):
                if(currentSchema.required ):
                    # If empty and required return field name and error
                    recordFailed = True
                    failedRules.append([fieldName, ValidationError.requiredError, "", ""])
                    continue
                else:
                    # If field is empty and not required its valid
                    checkRequiredOnly = True

            # Always check the type in the schema
            if(not checkRequiredOnly and not Validator.checkType(currentData,currentSchema.field_type.name) ) :
                recordTypeFailure = True
                recordFailed = True
                failedRules.append([fieldName, ValidationError.typeError, currentData,""])
                # Don't check value rules if type failed
                continue

            # Check length based on schema
            if currentSchema.length is not None and currentData is not None and len(currentData.strip()) > currentSchema.length:
                # Length failure, add to failedRules
                recordFailed = True
                failedRules.append([fieldName, ValidationError.lengthError, currentData,""])

        return (not recordFailed), failedRules, (not recordTypeFailure)

    @staticmethod
    def checkType(data,datatype) :
        """ Determine whether data is of the correct type

        Args:
            data: Data to be checked
            datatype: Type to check against

        Returns:
            True if data is of specified type, False otherwise
        """
        if datatype is None:
            # If no type specified, don't need to check anything
            return True
        if(data.strip() == ""):
            # An empty string matches all types
            return True
        if(datatype == "STRING") :
            return(len(data) > 0)
        if(datatype == "BOOLEAN") :
            if(data.upper() in Validator.BOOLEAN_VALUES) :
                return True
            return False
        if(datatype == "INT") :
            try:
                int(data)
                return True
            except:
                return False
        if(datatype == "DECIMAL") :
            try:
                Decimal(data)
                return True
            except:
                return False
        if(datatype == "LONG"):
            try:
                long(data)
                return True
            except:
                return False
        raise ValueError("".join(["Data Type Error, Type: ",datatype,", Value: ",data]))

    @staticmethod
    def padToLength(data,padLength):
        """ Pad data with leading zeros

        Args:
            data: string to be padded
            padLength: length of string after padding

        Returns:
            padded string of length padLength
        """
        if data is None:
            # Convert None to empty string so it can be padded with zeros
            return data
        data = data.strip()
        if data == "":
            # Empty values treated as null
            return None
        if len(data) <= padLength:
            return data.zfill(padLength)
        else:
            raise ValueError("".join(["Value is too long: ",str(data)]))

    @classmethod
    def validateFileBySql(cls, submissionId, fileType, interfaces):
        """ Check all SQL rules

        Args:
            submissionId: submission to be checked
            fileType: file type being checked
            interfaces: database interface objects

        Returns:
            List of errors found, each element has:
             field names
             error message
             values in fields involved
             row number
        """

        CloudLogger.logError("VALIDATOR_INFO: ", "Beginning SQL validation rules on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")

        # Pull all SQL rules for this file type
        fileId = interfaces.validationDb.getFileTypeIdByName(fileType)
        rules = interfaces.validationDb.session.query(RuleSql).filter(RuleSql.file_id == fileId).filter(
            RuleSql.rule_cross_file_flag == False).all()
        errors = []

        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        # For each rule, execute sql for rule
        for rule in rules:
            CloudLogger.logError("VALIDATOR_INFO: ", "Running query: "+str(RuleSql.query_name)+" on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")
            failures = interfaces.stagingDb.connection.execute(rule.rule_sql.format(submissionId))
            if failures.rowcount:
                # Create column list (exclude row_number)
                cols = failures.keys()
                cols.remove("row_number")
                # Build error list
                for failure in failures:
                    errorMsg = rule.rule_error_message
                    row = failure["row_number"]
                    # Create strings for fields and values
                    valueList = ["{}: {}".format(shortColnames[field], str(failure[field])) if field in shortColnames else "{}: {}".format(field, str(failure[field])) for field in cols]
                    valueString = ", ".join(valueList)
                    fieldList = [shortColnames[field] if field in shortColnames else field for field in cols]
                    fieldString = ", ".join(fieldList)
                    errors.append([fieldString, errorMsg, valueString, row, rule.rule_label, fileId, rule.target_file_id])

            # Pull where clause out of rule
            wherePosition = rule.rule_sql.lower().find("where")
            whereClause = rule.rule_sql[wherePosition:].format(submissionId)
            # Find table to apply this to
            model = interfaces.stagingDb.getModel(fileType)
            tableName = model.__tablename__
            tableAbbrev = cls.tableAbbreviations[tableName]
            # Update valid_record to false for all that fail this rule
            updateQuery = "UPDATE {} as {} SET valid_record = false {}".format(tableName,tableAbbrev,whereClause)
            interfaces.stagingDb.connection.execute(updateQuery)

            CloudLogger.logError("VALIDATOR_INFO: ", "Completed SQL validation rules on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")

        return errors
