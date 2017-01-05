from decimal import Decimal
import logging

from dataactcore.models.lookups import (FIELD_TYPE_DICT_ID, FILE_TYPE_DICT_ID, FILE_TYPE_DICT)
from dataactcore.models.stagingModels import FlexField
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.interfaces.db import GlobalDB

logger = logging.getLogger(__name__)

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
    def validate(cls, record, csv_schema):
        """
        Run initial set of single file validation:
        - check if required fields are present
        - check if data type matches data type specified in schema
        - check that field length matches field length specified in schema

        Args:
        record -- dict representation of a single record of data
        csv_schema -- dict of schema for the current file.

        Returns:
        Tuple of three values:
        True if validation passed, False if failed
        List of failed rules, each with field, description of failure, value that failed, rule label, and severity
        True if type check passed, False if type failed
        """
        record_failed = False
        record_type_failure = False
        failed_rules = []

        for field_name in csv_schema:
            if csv_schema[field_name].required and field_name not in record:
                return False, [[field_name, ValidationError.requiredError, "", "", "fatal"]], False

        total_fields = 0
        blank_fields = 0
        for field_name in record :
            if field_name in cls.META_FIELDS:
                # Skip fields that are not user submitted
                continue
            check_required_only = False
            current_schema = csv_schema[field_name]
            total_fields += 1

            current_data = record[field_name]
            if current_data is not None:
                current_data = current_data.strip()

            if current_data is None or len(current_data) == 0:
                blank_fields += 1
                if current_schema.required:
                    # If empty and required return field name and error
                    record_failed = True
                    failed_rules.append([field_name, ValidationError.requiredError, "", "", "fatal"])
                    continue
                else:
                    # If field is empty and not required its valid
                    check_required_only = True

            # Always check the type in the schema
            if not check_required_only and not Validator.checkType(current_data,
                                                                 FIELD_TYPE_DICT_ID[current_schema.field_types_id]):
                record_type_failure = True
                record_failed = True
                failed_rules.append([field_name, ValidationError.typeError, current_data,"", "fatal"])
                # Don't check value rules if type failed
                continue

            # Check length based on schema
            if current_schema.length is not None and current_data is not None and len(current_data.strip()) > current_schema.length:
                # Length failure, add to failedRules
                record_failed = True
                failed_rules.append([field_name, ValidationError.lengthError, current_data,"", "warning"])

        # if all columns are blank (empty row), set it so it doesn't add to the error messages or write the line, just ignore it
        if total_fields == blank_fields:
            record_failed = False
            record_type_failure = True
        return (not record_failed), failed_rules, (not record_type_failure)

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
        if data.strip() == "":
            # An empty string matches all types
            return True
        if datatype == "STRING":
            return len(data) > 0
        if datatype == "BOOLEAN":
            if data.upper() in Validator.BOOLEAN_VALUES:
                return True
            return False
        if datatype == "INT":
            try:
                int(data)
                return True
            except:
                return False
        if datatype == "DECIMAL":
            try:
                Decimal(data)
                return True
            except:
                return False
        if datatype == "LONG":
            try:
                int(data)
                return True
            except:
                return False
        raise ValueError("".join(["Data Type Error, Type: ",datatype,", Value: ",data]))

    @classmethod
    def validateFileBySql(cls, submission_id, fileType, short_to_long_dict):
        """ Check all SQL rules

        Args:
            submission_id: submission to be checked
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

        logger.info(
            'VALIDATOR_INFO: Beginning SQL validation rules on submissionID '
            '%s, fileType: %s', submission_id, fileType)
        sess = GlobalDB.db().session

        # Pull all SQL rules for this file type
        fileId = FILE_TYPE_DICT[fileType]
        rules = sess.query(RuleSql).filter_by(
            file_id=fileId, rule_cross_file_flag=False)
        errors = []

        # For each rule, execute sql for rule
        for rule in rules:
            logger.info(
                'VALIDATOR_INFO: Running query: %s on submissionId %s, '
                'fileType: %s', rule.query_name, submission_id, fileType)
            failures = sess.execute(rule.rule_sql.format(submission_id))
            if failures.rowcount:
                # Create column list (exclude row_number)
                cols = failures.keys()
                cols.remove("row_number")

                # Create flex column list
                flex_dict = {}
                flex_results = sess.query(FlexField).filter_by(submission_id=submission_id)

                for flex_row in flex_results:
                    flex_dict[flex_row.row_number] = flex_row

                # Build error list
                for failure in failures:
                    errorMsg = rule.rule_error_message
                    row = failure["row_number"]
                    # Create strings for fields and values
                    valueList = ["{}: {}".format(short_to_long_dict[field], str(failure[field])) if field in short_to_long_dict else "{}: {}".format(field, str(failure[field])) for field in cols]
                    if flex_dict and flex_dict[row]:
                        valueList.append("{}: {}".format(flex_dict[row].header, flex_dict[row].cell))
                    valueString = ", ".join(valueList)
                    fieldList = [short_to_long_dict[field] if field in short_to_long_dict else field for field in cols]
                    if flex_dict and flex_dict[row]:
                        fieldList.append(flex_dict[row].header)
                    fieldString = ", ".join(fieldList)
                    errors.append([fieldString, errorMsg, valueString, row, rule.rule_label, fileId, rule.target_file_id, rule.rule_severity_id])

            logger.info(
                'VALIDATOR_INFO: Completed SQL validation rules on '
                'submissionID: %s, fileType: %s', submission_id, fileType)

        return errors
