from collections import defaultdict, namedtuple
from decimal import Decimal, DecimalException
from datetime import datetime
import logging

from dataactcore.models.lookups import FIELD_TYPE_DICT_ID, FILE_TYPE_DICT_ID, FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER
from dataactcore.models.stagingModels import FlexField
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.interfaces.db import GlobalDB

logger = logging.getLogger(__name__)

Failure = namedtuple('Failure', ['field', 'description', 'value', 'label', 'severity'])
ValidationFailure = namedtuple('ValidationFailure', ['field_name', 'error', 'failed_value', 'row', 'original_label',
                                                     'file_type_id', 'target_file_id', 'severity_id'])


class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE", "FALSE", "YES", "NO", "1", "0"]
    tableAbbreviations = {"appropriations": "approp", "award_financial_assistance": "afa", "award_financial": "af",
                          "object_class_program_activity": "op", "appropriation": "approp"}
    # Set of metadata fields that should not be directly validated
    META_FIELDS = ["row_number"]

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
        List of Failure tuples
        True if type check passed, False if type failed
        """
        record_failed = False
        record_type_failure = False
        failed_rules = []

        for field_name in csv_schema:
            if csv_schema[field_name].required and field_name not in record:
                return (
                    False,
                    [Failure(field_name, ValidationError.requiredError, "", "", "fatal")],
                    False
                )

        total_fields = 0
        blank_fields = 0
        for field_name in record:
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
                    failed_rules.append(Failure(field_name, ValidationError.requiredError, "", "", "fatal"))
                    continue
                else:
                    # If field is empty and not required its valid
                    check_required_only = True

            # Always check the type in the schema
            if not check_required_only and not Validator.check_type(current_data,
                                                                    FIELD_TYPE_DICT_ID[current_schema.field_types_id]):
                record_type_failure = True
                record_failed = True
                failed_rules.append(Failure(field_name, ValidationError.typeError, current_data, "", "fatal"))
                # Don't check value rules if type failed
                continue

            # Check length based on schema
            if current_schema.length is not None and current_data is not None and \
               len(current_data.strip()) > current_schema.length:
                # Length failure, add to failedRules
                record_failed = True
                failed_rules.append(Failure(field_name, ValidationError.lengthError, current_data, "", "warning"))

        # if all columns are blank (empty row), set it so it doesn't add to the error messages or write the line,
        # just ignore it
        if total_fields == blank_fields:
            record_failed = False
            record_type_failure = True
        return (not record_failed), failed_rules, (not record_type_failure)

    @staticmethod
    def check_type(data, datatype):
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
            except ValueError:
                return False
        if datatype == "DECIMAL":
            try:
                Decimal(data)
                return True
            except DecimalException:
                return False
        if datatype == "LONG":
            try:
                int(data)
                return True
            except ValueError:
                return False
        raise ValueError("".join(["Data Type Error, Type: ", datatype, ", Value: ", data]))


def cross_validate_sql(rules, submission_id, short_to_long_dict, first_file, second_file, job):
    """ Evaluate all sql-based rules for cross file validation

    Args:
        rules -- List of Rule objects
        submission_id -- ID of submission to run cross-file validation
    """
    failures = []
    # Put each rule through evaluate, appending all failures into list
    conn = GlobalDB.db().connection

    for rule in rules:

        rule_start = datetime.now()
        logger.info(
            {
                'message': 'Beginning cross-file rule '+rule.query_name+' on submission_id: '+str(submission_id),
                'message_type': 'ValidatorInfo',
                'rule': rule.query_name,
                'job_id': job.job_id,
                'submission_id': submission_id,
                'action': 'run_cross_validation_rule',
                'status': 'start',
                'start': rule_start})
        failed_rows = conn.execute(
            rule.rule_sql.format(submission_id))
        if failed_rows.rowcount:
            # get list of fields involved in this validation
            # note: row_number is metadata, not a field being
            # validated, so exclude it
            cols = failed_rows.keys()
            cols.remove('row_number')
            column_string = ", ".join(short_to_long_dict[c] if c in short_to_long_dict else c for c in cols)

            # materialize as we'll iterate over the failed_rows twice
            failed_rows = list(failed_rows)
            flex_data = relevant_cross_flex_data(failed_rows, submission_id, [first_file, second_file])

            for row in failed_rows:
                # get list of values for each column
                values = ["{}: {}".format(short_to_long_dict[c], str(row[c])) if c in short_to_long_dict else
                          "{}: {}".format(c, str(row[c])) for c in cols]
                values = ", ".join(values)
                full_column_string = column_string
                # go through all flex fields in this row and add to the columns and values
                for field in flex_data[row['row_number']]:
                    full_column_string += ", " + field.header + "_file" +\
                                          FILE_TYPE_DICT_LETTER[field.file_type_id].lower()
                    values += ", {}: {}".format(field.header + "_file" +
                                                FILE_TYPE_DICT_LETTER[field.file_type_id].lower(), field.cell)

                target_file_type = FILE_TYPE_DICT_ID[rule.target_file_id]
                failures.append([rule.file.name, target_file_type, full_column_string,
                                str(rule.rule_error_message), values, row['row_number'], str(rule.rule_label),
                                rule.file_id, rule.target_file_id, rule.rule_severity_id])

        rule_duration = (datetime.now()-rule_start).total_seconds()
        logger.info(
            {
                'message': 'Completed cross-file rule '+rule.query_name+' on submission_id: '+str(submission_id),
                'message_type': 'ValidatorInfo',
                'rule': rule.query_name,
                'job_id': job.job_id,
                'submission_id': submission_id,
                'action': 'run_cross_validation_rule',
                'status': 'finish',
                'start': rule_start,
                'duration': rule_duration})

    # Return list of cross file validation failures
    return failures


def validate_file_by_sql(job, file_type, short_to_long_dict):
    """ Check all SQL rules

    Args:
        job: the Job which is running
        file_type: file type being checked
        short_to_long_dict: mapping of short to long schema column names

    Returns:
        List of ValidationFailures
    """

    logger.info('VALIDATOR_INFO: Beginning SQL validation rules on job %s, (submission: %s, fileType: %s)',
                job.job_id, job.submission_id, file_type)
    sess = GlobalDB.db().session

    # Pull all SQL rules for this file type
    file_id = FILE_TYPE_DICT[file_type]
    rules = sess.query(RuleSql).filter_by(file_id=file_id, rule_cross_file_flag=False)
    errors = []

    # For each rule, execute sql for rule
    for rule in rules:
        logger.info('VALIDATOR_INFO: Running query: %s on job %s', rule.query_name, job.job_id)
        failures = sess.execute(rule.rule_sql.format(job.submission_id))
        if failures.rowcount:
            # Create column list (exclude row_number)
            cols = failures.keys()
            cols.remove("row_number")
            col_headers = [short_to_long_dict.get(field, field) for field in cols]

            # materialize as we'll iterate over the failures twice
            failures = list(failures)
            flex_data = relevant_flex_data(failures, job.job_id)

            errors.extend(failure_row_to_tuple(rule, flex_data, cols, col_headers, file_id, failure)
                          for failure in failures)

        logger.info('VALIDATOR_INFO: Completed SQL validation query %s on job %s', rule.query_name, job.job_id)

    return errors


def relevant_flex_data(failures, job_id):
    """Create a dictionary mapping row numbers of failures to lists of
    FlexFields"""
    sess = GlobalDB.db().session
    flex_data = defaultdict(list)
    relevant_rows = {f['row_number'] for f in failures}
    query = sess.query(FlexField).filter(FlexField.row_number.in_(relevant_rows), FlexField.job_id == job_id).\
        order_by(FlexField.flex_field_id)
    for flex_field in query:
        flex_data[flex_field.row_number].append(flex_field)
    return flex_data


def relevant_cross_flex_data(failed_rows, submission_id, files):
    """Create a dictionary mapping row numbers of cross-file failures to lists of FlexFields"""
    sess = GlobalDB.db().session
    flex_data = defaultdict(list)
    relevant_rows = {f['row_number'] for f in failed_rows}
    query = sess.query(FlexField).filter(FlexField.row_number.in_(relevant_rows),
                                         FlexField.submission_id == submission_id,
                                         FlexField.file_type_id.in_(files)). \
        order_by(FlexField.flex_field_id)
    for flex_field in query:
        flex_data[flex_field.row_number].append(flex_field)
    return flex_data


def failure_row_to_tuple(rule, flex_data, cols, col_headers, file_id, sql_failure):
    """Convert a failure SQL row into a ValidationFailure"""
    row = sql_failure["row_number"]
    # Create strings for fields and values
    values_list = ["{}: {}".format(header, str(sql_failure[field])) for field, header in zip(cols, col_headers)]
    values_list.extend("{}: {}".format(flex_field.header, flex_field.cell) for flex_field in flex_data[row])
    field_list = col_headers + [field.header for field in flex_data[row]]
    return ValidationFailure(
        ", ".join(field_list),
        rule.rule_error_message,
        ", ".join(values_list),
        row,
        rule.rule_label,
        file_id,
        rule.target_file_id,
        rule.rule_severity_id
    )
