from decimal import Decimal
import logging

from dataactcore.models.lookups import (FIELD_TYPE_DICT_ID, FILE_TYPE_DICT_ID, FILE_TYPE_DICT)
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.interfaces.db import GlobalDB


_exception_logger = logging.getLogger('deprecated.exception')


class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE","FALSE","YES","NO","1","0"]
    tableAbbreviations = {"appropriations":"approp","award_financial_assistance":"afa","award_financial":"af","object_class_program_activity":"op","appropriation":"approp"}
    # Set of metadata fields that should not be directly validated
    META_FIELDS = ["row_number"]

    @classmethod
    def crossValidateSql(cls, rules, submissionId, short_to_long_dict):
        """ Evaluate all sql-based rules for cross file validation

        Args:
            rules -- List of Rule objects
            submissionId -- ID of submission to run cross-file validation
        """
        failures = []
        # Put each rule through evaluate, appending all failures into list
        # Put each rule through evaluate, appending all failures into list
        conn = GlobalDB.db().connection

        for rule in rules:
            failedRows = conn.execute(
                rule.rule_sql.format(submissionId))
            if failedRows.rowcount:
                # get list of fields involved in this validation
                # note: row_number is metadata, not a field being
                # validated, so exclude it
                cols = failedRows.keys()
                cols.remove('row_number')
                columnString = ", ".join(short_to_long_dict[c] if c in short_to_long_dict else c for c in cols)
                for row in failedRows:
                    # get list of values for each column
                    values = ["{}: {}".format(short_to_long_dict[c], str(row[c])) if c in short_to_long_dict else "{}: {}".format(c, str(row[c])) for c in cols]
                    values = ", ".join(values)
                    targetFileType = FILE_TYPE_DICT_ID[rule.target_file_id]
                    failures.append([rule.file.name, targetFileType, columnString,
                        str(rule.rule_error_message), values, row['row_number'],str(rule.rule_label),rule.file_id,rule.target_file_id,rule.rule_severity_id])

        # Return list of cross file validation failures
        return failures

    @classmethod
    def validate(cls, record, csvSchema):
        """
        Run initial set of single file validation:
        - check if required fields are present
        - check if data type matches data type specified in schema
        - check that field length matches field length specified in schema

        Args:
        record -- dict representation of a single record of data
        csvSchema -- dict of schema for the current file.

        Returns:
        Tuple of three values:
        True if validation passed, False if failed
        List of failed rules, each with field, description of failure, value that failed, rule label, and severity
        True if type check passed, False if type failed
        """
        recordFailed = False
        recordTypeFailure = False
        failedRules = []

        for fieldName in csvSchema:
            if (csvSchema[fieldName].required and not fieldName in record):
                return False, [[fieldName, ValidationError.requiredError, "", "", "fatal"]], False

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
                    failedRules.append([fieldName, ValidationError.requiredError, "", "", "fatal"])
                    continue
                else:
                    # If field is empty and not required its valid
                    checkRequiredOnly = True

            # Always check the type in the schema
            if not checkRequiredOnly and not Validator.checkType(currentData,
                                                                 FIELD_TYPE_DICT_ID[currentSchema.field_types_id]):
                recordTypeFailure = True
                recordFailed = True
                failedRules.append([fieldName, ValidationError.typeError, currentData,"", "fatal"])
                # Don't check value rules if type failed
                continue

            # Check length based on schema
            if currentSchema.length is not None and currentData is not None and len(currentData.strip()) > currentSchema.length:
                # Length failure, add to failedRules
                recordFailed = True
                failedRules.append([fieldName, ValidationError.lengthError, currentData,"", "warning"])

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
                int(data)
                return True
            except:
                return False
        raise ValueError("".join(["Data Type Error, Type: ",datatype,", Value: ",data]))

    @classmethod
    def validateFileBySql(cls, submissionId, fileType, short_to_long_dict):
        """ Check all SQL rules

        Args:
            submissionId: submission to be checked
            fileType: file type being checked
            short_to_long_dict: mapping of short to long schema column names

        Returns:
            List of errors found, each element has:
             field names
             error message
             values in fields involved
             row number
             rule label
             source file id
             target file id
             severity id
        """

        _exception_logger.info(
            'VALIDATOR_INFO: Beginning SQL validation rules on submissionID '
            '%s, fileType: %s', submissionId, fileType)
        sess = GlobalDB.db().session

        # Pull all SQL rules for this file type
        fileId = FILE_TYPE_DICT[fileType]
        rules = sess.query(RuleSql).filter(RuleSql.file_id == fileId).filter(
            RuleSql.rule_cross_file_flag == False).all()
        errors = []

        # For each rule, execute sql for rule
        for rule in rules:
            _exception_logger.info(
                'VALIDATOR_INFO: Running query: %s on submissionId %s, '
                'fileType: %s', rule.query_name, submissionId, fileType)
            failures = sess.execute(rule.rule_sql.format(submissionId))
            if failures.rowcount:
                # Create column list (exclude row_number)
                cols = failures.keys()
                cols.remove("row_number")
                # Build error list
                for failure in failures:
                    errorMsg = rule.rule_error_message
                    row = failure["row_number"]
                    # Create strings for fields and values
                    valueList = ["{}: {}".format(short_to_long_dict[field], str(failure[field])) if field in short_to_long_dict else "{}: {}".format(field, str(failure[field])) for field in cols]
                    valueString = ", ".join(valueList)
                    fieldList = [short_to_long_dict[field] if field in short_to_long_dict else field for field in cols]
                    fieldString = ", ".join(fieldList)
                    errors.append([fieldString, errorMsg, valueString, row, rule.rule_label, fileId, rule.target_file_id, rule.rule_severity_id])

            _exception_logger.info(
                'VALIDATOR_INFO: Completed SQL validation rules on '
                'submissionID: %s, fileType: %s', submissionId, fileType)

        return errors
