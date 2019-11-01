from collections import defaultdict, namedtuple
from decimal import Decimal, DecimalException
from datetime import datetime
import logging

from dataactcore.models.lookups import FIELD_TYPE_DICT_ID, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import RuleSql
from dataactcore.models.domainModels import concat_display_tas_dict
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.interfaces.db import GlobalDB

logger = logging.getLogger(__name__)

Failure = namedtuple('Failure', ['unique_id', 'field', 'description', 'value', 'label', 'expected', 'severity'])
ValidationFailure = namedtuple('ValidationFailure', ['unique_id', 'field_name', 'error', 'failed_value',
                                                     'expected_value', 'difference', 'flex_fields', 'row',
                                                     'original_label', 'file_type_id', 'target_file_id', 'severity_id'])


class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE", "FALSE", "YES", "NO", "1", "0"]
    tableAbbreviations = {"appropriations": "approp", "award_financial_assistance": "afa", "award_financial": "af",
                          "object_class_program_activity": "op", "appropriation": "approp"}
    # Set of metadata fields that should not be directly validated
    META_FIELDS = ["row_number", "afa_generated_unique", "unique_award_key"]

    @classmethod
    def validate(cls, record, csv_schema, fabs_record=False, required_labels=None, type_labels=None):
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

        if not fabs_record:
            unique_id = 'TAS: {}'.format(concat_display_tas_dict(record))
        else:
            unique_id = 'AssistanceTransactionUniqueKey: {}'.format(record['afa_generated_unique'])

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
                    # if it's a FABS record and the required column is in the list, label it specifically
                    if fabs_record and required_labels and current_schema.name_short in required_labels:
                        failed_rules.append(Failure(unique_id, field_name, ValidationError.requiredError, '',
                                                    required_labels[current_schema.name_short], '(not blank)', 'fatal'))
                    else:
                        failed_rules.append(Failure(unique_id, field_name, ValidationError.requiredError, '', '',
                                                    '(not blank)', 'fatal'))
                    continue
                else:
                    # If field is empty and not required its valid
                    check_required_only = True

            current_type = FIELD_TYPE_DICT_ID[current_schema.field_types_id]
            # Always check the type in the schema
            if not check_required_only and not Validator.check_type(current_data, current_type):
                record_type_failure = True
                record_failed = True
                # if it's a FABS record and the type column is in the list, label it specifically
                if fabs_record and type_labels and current_schema.name_short in type_labels:
                    failed_rules.append(Failure(unique_id, field_name, ValidationError.typeError, current_data,
                                                type_labels[current_schema.name_short],
                                                'This field must be a {}'.format(current_type.lower()), 'fatal'))
                else:
                    failed_rules.append(Failure(unique_id, field_name, ValidationError.typeError, current_data, '',
                                                'This field must be a {}'.format(current_type.lower()), 'fatal'))
                # Don't check value rules if type failed
                continue

            # Check length based on schema
            if current_schema.length is not None and current_data is not None and \
               len(current_data.strip()) > current_schema.length:
                # Length failure, add to failedRules
                record_failed = True
                warning_type = 'fatal' if fabs_record else 'warning'
                failed_rules.append(Failure(unique_id, field_name, ValidationError.lengthError, current_data, '',
                                            'Max length: {}'.format(current_schema.length),
                                            warning_type))

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


def cross_validate_sql(rules, submission_id, short_to_long_dict, job_id, error_csv, warning_csv, error_list):
    """ Evaluate all sql-based rules for cross file validation

        Args:
            rules: list of Rule objects
            submission_id: ID of submission to run cross-file validation on
            short_to_long_dict: mapping of short to long schema column names
            job_id: the id of the cross-file job
            error_csv: the csv to write errors to
            warning_csv: the csv to write warnings to
            error_list: instance of ErrorInterface to keep track of errors
    """
    conn = GlobalDB.db().connection

    # Put each rule through evaluate, appending all failures into list
    for rule in rules:
        rule_start = datetime.now()
        logger.info({
            'message': 'Beginning cross-file rule {} on submission_id: {}'.format(rule.query_name, str(submission_id)),
            'message_type': 'ValidatorInfo',
            'rule': rule.query_name,
            'job_id': job_id,
            'submission_id': submission_id,
            'action': 'run_cross_validation_rule',
            'status': 'start',
            'start': rule_start
        })
        failed_rows = conn.execute(rule.rule_sql.format(submission_id))
        logger.info({
            'message': 'Finished running cross-file rule {} on submission_id: {}.'.format(rule.query_name,
                                                                                          str(submission_id)) +
                       'Starting flex field gathering and file writing',
            'message_type': 'ValidatorInfo',
            'rule': rule.query_name,
            'job_id': job_id,
            'submission_id': submission_id
        })
        if failed_rows.rowcount:
            rule_cols = failed_rows.keys()
            # get list of fields involved in this validation
            source_len = len('source_value_')
            target_len = len('target_value_')
            source_cols = []
            target_cols = []
            for col in rule_cols:
                if col.startswith('source_value_'):
                    source_cols.append(col)
                elif col.startswith('target_value_'):
                    target_cols.append(col)
            source_headers = [short_to_long_dict.get(field[source_len:], field[source_len:]) for field in source_cols]
            target_headers = [short_to_long_dict.get(field[target_len:], field[target_len:]) for field in target_cols]

            # materialize as we'll iterate over the failed_rows twice
            failed_rows = list(failed_rows)
            num_failed_rows = len(failed_rows)
            slice_start = 0
            slice_size = 10000
            while slice_start <= num_failed_rows:
                # finding out row numbers for logger
                last_error_curr_slice = slice_start + slice_size
                failed_row_subset = failed_rows[slice_start:last_error_curr_slice]
                if last_error_curr_slice > num_failed_rows:
                    last_error_curr_slice = num_failed_rows
                logger.info({
                    'message': 'Starting flex field gathering for cross-file rule ' +
                               '{} on submission_id: {} for '.format(rule.query_name, str(submission_id)) +
                               'failure rows: {}-{}'.format(str(slice_start), str(last_error_curr_slice)),
                    'message_type': 'ValidatorInfo',
                    'rule': rule.query_name,
                    'job_id': job_id,
                    'submission_id': submission_id
                })
                source_flex_data = relevant_cross_flex_data(failed_row_subset, submission_id, rule.file_id)
                logger.info({
                    'message': 'Finished flex field gathering for cross-file rule ' +
                               '{} on submission_id: {} for '.format(rule.query_name, str(submission_id)) +
                               'failure rows: {}-{}'.format(str(slice_start), str(last_error_curr_slice)),
                    'message_type': 'ValidatorInfo',
                    'rule': rule.query_name,
                    'job_id': job_id,
                    'submission_id': submission_id
                })

                for row in failed_row_subset:
                    # Getting row numbers
                    source_row_number = row['source_row_number'] if 'source_row_number' in rule_cols else ''

                    # get list of values for each column
                    source_values = ['{}: {}'.format(short_to_long_dict.get(c[source_len:], c[source_len:]),
                                                     str(row[c] or '')) for c in source_cols]
                    target_values = ['{}: {}'.format(short_to_long_dict.get(c[target_len:], c[target_len:]),
                                                     str(row[c] or '')) for c in target_cols]

                    # Getting all flex fields organized
                    source_flex_list = []
                    if source_flex_data:
                        source_flex_list = ['{}: {}'.format(flex_field.header, flex_field.cell or '')
                                            for flex_field in source_flex_data[source_row_number]]

                    # Getting the difference and unique IDs
                    difference = ''
                    diff_start = 'difference_'
                    unique_start = 'uniqueid_'
                    diff_array = []
                    unique_key = []
                    for failure_key in rule_cols:
                        # Difference
                        if failure_key == 'difference':
                            difference = str(row[failure_key] or '')
                        if failure_key.startswith(diff_start):
                            diff_header = failure_key[len(diff_start):]
                            diff_array.append('{}: {}'.format(diff_header, str(row[failure_key] or '')))
                        # Unique key
                        if failure_key.startswith(unique_start):
                            unique_header = failure_key[len(unique_start):]
                            unique_key.append('{}: {}'.format(unique_header, str(row[failure_key] or '')))
                    # If we have multiple differences, join them
                    if diff_array:
                        difference = ', '.join(diff_array)

                    # Creating a failure array for both writing the row and recording the error metadata
                    failure = [', '.join(unique_key), rule.file.name, ', '.join(source_headers), rule.target_file.name,
                               ', '.join(target_headers), str(rule.rule_error_message), ', '.join(source_values),
                               ', '.join(target_values), difference, ', '.join(source_flex_list), source_row_number,
                               str(rule.rule_label), rule.file_id, rule.target_file_id, rule.rule_severity_id]
                    if failure[14] == RULE_SEVERITY_DICT['fatal']:
                        error_csv.writerow(failure[0:12])
                    if failure[14] == RULE_SEVERITY_DICT['warning']:
                        warning_csv.writerow(failure[0:12])
                    error_list.record_row_error(job_id, 'cross_file', failure[1], failure[5], failure[10], failure[11],
                                                failure[12], failure[13], severity_id=failure[14])
                slice_start = slice_start + slice_size

        rule_duration = (datetime.now()-rule_start).total_seconds()
        logger.info({
            'message': 'Completed cross-file rule {} on submission_id: {}'.format(rule.query_name, str(submission_id)),
            'message_type': 'ValidatorInfo',
            'rule': rule.query_name,
            'job_id': job_id,
            'submission_id': submission_id,
            'action': 'run_cross_validation_rule',
            'status': 'finish',
            'start': rule_start,
            'duration': rule_duration
        })


def validate_file_by_sql(job, file_type, short_to_long_dict):
    """ Check all SQL rules

    Args:
        job: the Job which is running
        file_type: file type being checked
        short_to_long_dict: mapping of short to long schema column names

    Returns:
        List of ValidationFailures
    """

    sql_val_start = datetime.now()
    log_string = 'on submission_id: {}, job_id: {}, file_type: {}'.format(str(job.submission_id), str(job.job_id),
                                                                          job.file_type.name)
    logger.info({
        'message': 'Beginning SQL validations {}'.format(log_string),
        'message_type': 'ValidatorInfo',
        'submission_id': job.submission_id,
        'job_id': job.job_id,
        'file_type': job.file_type.name,
        'action': 'run_sql_validations',
        'status': 'start',
        'start_time': sql_val_start
    })
    sess = GlobalDB.db().session

    # Pull all SQL rules for this file type
    file_id = FILE_TYPE_DICT[file_type]
    rules = sess.query(RuleSql).filter_by(file_id=file_id, rule_cross_file_flag=False)
    errors = []

    # For each rule, execute sql for rule
    for rule in rules:
        rule_start = datetime.now()
        logger.info({
            'message': 'Beginning SQL validation rule {} {}'.format(rule.query_name, log_string),
            'message_type': 'ValidatorInfo',
            'submission_id': job.submission_id,
            'job_id': job.job_id,
            'rule': rule.query_name,
            'file_type': job.file_type.name,
            'action': 'run_sql_validation_rule',
            'status': 'start',
            'start_time': rule_start
        })

        failures = sess.execute(rule.rule_sql.format(job.submission_id))
        if failures.rowcount:
            # Create column list (exclude row_number)
            cols = []
            exact_names = ['row_number', 'difference']
            starting = ('expected_value_', 'uniqueid_')
            for col in failures.keys():
                if col not in exact_names and not col.startswith(starting):
                    cols.append(col)
            col_headers = [short_to_long_dict.get(field, field) for field in cols]

            # materialize as we'll iterate over the failures twice
            failures = list(failures)
            flex_data = relevant_flex_data(failures, job.job_id)

            errors.extend(failure_row_to_tuple(rule, flex_data, cols, col_headers, file_id, failure)
                          for failure in failures)

        rule_duration = (datetime.now() - rule_start).total_seconds()
        logger.info({
            'message': 'Completed SQL validation rule {} {}'.format(rule.query_name, log_string),
            'message_type': 'ValidatorInfo',
            'submission_id': job.submission_id,
            'job_id': job.job_id,
            'rule': rule.query_name,
            'file_type': job.file_type.name,
            'action': 'run_sql_validation_rule',
            'status': 'finish',
            'start_time': rule_start,
            'end_time': datetime.now(),
            'duration': rule_duration
        })

    sql_val_duration = (datetime.now()-sql_val_start).total_seconds()
    logger.info({
        'message': 'Completed SQL validations {}'.format(log_string),
        'message_type': 'ValidatorInfo',
        'submission_id': job.submission_id,
        'job_id': job.job_id,
        'file_type': job.file_type.name,
        'action': 'run_sql_validations',
        'status': 'finish',
        'start_time': sql_val_start,
        'end_time': datetime.now(),
        'duration': sql_val_duration
    })
    return errors


def relevant_flex_data(failures, job_id):
    """Create a dictionary mapping row numbers of failures to lists of
    FlexFields"""
    sess = GlobalDB.db().session
    flex_data = defaultdict(list)
    fail_string = "), (".join(str(f['row_number']) for f in failures if f['row_number'])
    # only do the rest of this gathering if there's any rows to search in the first place, there is at least
    # one rule that returns NULL for row_number
    if fail_string:
        # VALUES and EXISTS are ridiculous in sqlalchemy, using raw sql for this
        query = (
            "WITH all_values AS (SELECT * FROM (VALUES (" + fail_string + ")) as all_flexs (row_number)) " +
            "SELECT * " +
            "FROM flex_field " +
            "WHERE job_id=" + str(job_id) +
            " AND EXISTS (SELECT * FROM all_values WHERE flex_field.row_number = all_values.row_number)"
            "ORDER BY flex_field.header"
        )
        query_result = sess.execute(query)
        for flex_field in query_result:
            flex_data[flex_field.row_number].append(flex_field)
    return flex_data


def relevant_cross_flex_data(failed_rows, submission_id, file_id):
    """ Create a dictionary mapping row numbers of cross-file failures to lists of FlexFields

        Args:
            failed_rows: the subset of rows to get flex fields for
            submission_id: ID of the submission to get flex fields for
            file_id: the source file type ID of the cross-file rule for which to get flex fields

        Returns:
            A dict containing flex data for the source file in a cross-file validation
    """
    sess = GlobalDB.db().session
    flex_data = defaultdict(list)

    fail_string = '), ('.join(str(f['source_row_number']) for f in failed_rows if f['source_row_number'])
    # Only do this if we have any row numbers to check
    if fail_string:
        # VALUES and EXISTS are ridiculous in sqlalchemy, using raw sql for this
        query = """
            WITH all_values AS(SELECT * FROM(VALUES ({})) as all_flexs(row_number))
            SELECT *
            FROM flex_field
            WHERE submission_id = {}
            AND file_type_id = {}
            AND EXISTS (SELECT * FROM all_values WHERE flex_field.row_number = all_values.row_number)
        """
        query_result = sess.execute(query.format(fail_string, submission_id, file_id))
        for flex_field in query_result:
            flex_data[flex_field.row_number].append(flex_field)

    return flex_data


def failure_row_to_tuple(rule, flex_data, cols, col_headers, file_id, sql_failure):
    """ Convert a failure SQL row into a ValidationFailure.

        Args:
            rule: the RuleSql object representing the rule failed
            flex_data: the flex data for the subset of failures currently being converted into errors
            cols: the column values for the columns that are relevant to the error failed
            col_headers: the column names for the columns that are relevant to the error failed
            file_id: the ID number of the file being validated
            sql_failure: the failure returned from the SQL

        Returns:
            A ValidationFailure tuple that contains all values needed to write the row to the error report
    """
    row = sql_failure['row_number'] or ''

    # Determine the extra value for a rule
    expected_value = rule.expected_value
    difference = ''
    unique_id_start = 'uniqueid_'
    expect_start = 'expected_value_'

    unique_id_fields = []
    for failure_key in sql_failure.keys():
        # Expected value
        if failure_key.startswith(expect_start):
            fail_header = failure_key[len(expect_start):]
            expected_value = '{}: {}'.format(fail_header, (str(sql_failure[failure_key] or '')))
        # Difference
        elif failure_key == 'difference':
            difference = str(sql_failure[failure_key] or '')
        elif failure_key.startswith(unique_id_start):
            fail_header = failure_key[len(unique_id_start):]
            unique_id_fields.append('{}: {}'.format(fail_header, str(sql_failure[failure_key] or '')))

    # Create strings for fields and values
    values_list = ['{}: {}'.format(header, str(sql_failure[field] or '')) for field, header in zip(cols, col_headers)]
    flex_list = ['{}: {}'.format(flex_field.header, flex_field.cell or '') for flex_field in flex_data[row]]
    # Create unique id string
    unique_id = ', '.join(unique_id_fields)

    return ValidationFailure(
        unique_id,
        ', '.join(col_headers),
        rule.rule_error_message,
        ', '.join(values_list),
        expected_value,
        difference,
        ', '.join(flex_list),
        row,
        rule.rule_label,
        file_id,
        rule.target_file_id,
        rule.rule_severity_id
    )
