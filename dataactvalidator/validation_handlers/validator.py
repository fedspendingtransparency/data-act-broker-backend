from collections import defaultdict, namedtuple
from datetime import datetime
import logging

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import RuleSql
from dataactcore.interfaces.db import GlobalDB

logger = logging.getLogger(__name__)

Failure = namedtuple('Failure', ['unique_id', 'field', 'description', 'value', 'label', 'expected', 'severity'])
ValidationFailure = namedtuple('ValidationFailure', ['unique_id', 'field_name', 'error', 'failed_value',
                                                     'expected_value', 'difference', 'flex_fields', 'row',
                                                     'original_label', 'file_type_id', 'target_file_id', 'severity_id'])

SQL_VALIDATION_BATCH_SIZE = CONFIG_BROKER['validator_batch_size']


def cross_validate_sql(rules, submission_id, short_to_long_dict, job_id, error_csv, warning_csv, error_list,
                       batch_results=False):
    """ Evaluate all sql-based rules for cross file validation

        Args:
            rules: list of Rule objects
            submission_id: ID of submission to run cross-file validation on
            short_to_long_dict: mapping of short to long schema column names
            job_id: the id of the cross-file job
            error_csv: the csv to write errors to
            warning_csv: the csv to write warnings to
            error_list: instance of ErrorInterface to keep track of errors
            batch_results: instead of storing the results in memory, batch the results (for memory)
    """
    conn = GlobalDB.db().connection
    rules_start = datetime.now()

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

        def process_failures(failures, columns, batch_num=0):
            # get list of fields involved in this validation
            source_len = len('source_value_')
            target_len = len('target_value_')
            source_cols = []
            target_cols = []
            for col in columns:
                if col.startswith('source_value_'):
                    source_cols.append(col)
                elif col.startswith('target_value_'):
                    target_cols.append(col)
            source_headers = [short_to_long_dict.get(field[source_len:], field[source_len:]) for field in
                              source_cols]
            target_headers = [short_to_long_dict.get(field[target_len:], field[target_len:]) for field in
                              target_cols]

            # materialize as we'll iterate over the failed_rows twice
            failed_rows = list(failures)
            num_failed_rows = len(failed_rows)
            slice_size = 10000
            slice_start = (slice_size * batch_num)
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
                    source_row_number = row['source_row_number'] if 'source_row_number' in columns else ''

                    # get list of values for each column
                    source_values = ['{}: {}'.format(short_to_long_dict.get(c[source_len:], c[source_len:]),
                                                     str(row[c] if row[c] is not None else '')) for c in
                                     source_cols]
                    target_values = ['{}: {}'.format(short_to_long_dict.get(c[target_len:], c[target_len:]),
                                                     str(row[c] if row[c] is not None else '')) for c in
                                     target_cols]

                    # Getting all flex fields organized
                    source_flex_list = []
                    if source_flex_data:
                        source_flex_list = ['{}: {}'.format(flex_field.header,
                                                            flex_field.cell if flex_field.cell is not None else '')
                                            for flex_field in source_flex_data[source_row_number]]

                    # Getting the difference and unique IDs
                    difference = ''
                    diff_start = 'difference_'
                    unique_start = 'uniqueid_'
                    diff_array = []
                    unique_key = []
                    for failure_key in columns:
                        # Difference
                        if failure_key == 'difference':
                            difference = str(row[failure_key] if row[failure_key] is not None else '')
                        if failure_key.startswith(diff_start):
                            diff_header = failure_key[len(diff_start):]
                            diff_array.append('{}: {}'.format(diff_header, str(row[failure_key] if
                                                                               row[
                                                                                   failure_key] is not None else '')))
                        # Unique key
                        if failure_key.startswith(unique_start):
                            unique_header = failure_key[len(unique_start):]
                            unique_key.append('{}: {}'.format(unique_header, str(row[failure_key] if
                                                                                 row[
                                                                                     failure_key] is not None else '')))
                    # If we have multiple differences, join them
                    if diff_array:
                        difference = ', '.join(diff_array)

                    # Creating a failure array for both writing the row and recording the error metadata
                    failure = [', '.join(unique_key), rule.file.name, ', '.join(source_headers),
                               rule.target_file.name,
                               ', '.join(target_headers), str(rule.rule_error_message), ', '.join(source_values),
                               ', '.join(target_values), difference, ', '.join(source_flex_list), source_row_number,
                               str(rule.rule_label), rule.file_id, rule.target_file_id, rule.rule_severity_id]
                    if failure[14] == RULE_SEVERITY_DICT['fatal']:
                        error_csv.writerow(failure[0:12])
                    if failure[14] == RULE_SEVERITY_DICT['warning']:
                        warning_csv.writerow(failure[0:12])
                    error_list.record_row_error(job_id, 'cross_file', failure[1], failure[5], failure[10],
                                                failure[11],
                                                failure[12], failure[13], severity_id=failure[14])
                slice_start = slice_start + slice_size

        sub_rule_sql = rule.rule_sql.format(submission_id)
        if batch_results:
            # Only run the SQL in batches to save on memory
            proxy = conn.execution_options(stream_results=True).execute(sub_rule_sql)
            batch_num = 0
            while True:
                failures = proxy.fetchmany(SQL_VALIDATION_BATCH_SIZE)
                if not failures:
                    break
                process_failures(failures, failures[0].keys(), batch_num)
                batch_num += 1
            proxy.close()
        else:
            # Run the full SQL and fetch the results
            failures = conn.execute(sub_rule_sql)
            if failures.rowcount:
                process_failures(failures, failures.keys())

        rule_duration = (datetime.now() - rule_start).total_seconds()
        logger.info({
            'message': 'Finished processing cross-file rule {} on submission_id: {}.'.format(rule.query_name,
                                                                                             str(submission_id)),
            'message_type': 'ValidatorInfo',
            'rule': rule.query_name,
            'job_id': job_id,
            'submission_id': submission_id,
            'status': 'finish',
            'action': 'run_cross_validation_rule',
            'start': rule_start,
            'duration': rule_duration
        })

    rules_duration = (datetime.now() - rules_start).total_seconds()
    logger.info({
        'message': 'Completed cross-file rules on submission_id: {}'.format(str(submission_id)),
        'message_type': 'ValidatorInfo',
        'job_id': job_id,
        'submission_id': submission_id,
        'status': 'finish',
        'start': rules_start,
        'duration': rules_duration
    })


def validate_file_by_sql(job, file_type, short_to_long_dict, batch_results=False):
    """ Check all SQL rules

        Args:
            job: the Job which is running
            file_type: file type being checked
            short_to_long_dict: mapping of short to long schema column names
            batch_results: instead of storing the results in memory, batch the results (for memory)

        Yields:
            ValidationFailures
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
            'start_time': rule_start,
            'batch_results': batch_results
        })

        def process_batch(failures, columns):
            # Create column list (exclude row_number)
            cols = []
            exact_names = ['row_number', 'difference']
            starting = ('expected_value_', 'uniqueid_')
            for col in columns:
                if col not in exact_names and not col.startswith(starting):
                    cols.append(col)
            col_headers = [short_to_long_dict.get(field, field) for field in cols]

            # materialize as we'll iterate over the failures twice
            failures = list(failures)
            flex_data = relevant_flex_data(failures, job.job_id)

            for failure in failures:
                yield failure_row_to_tuple(rule, flex_data, cols, col_headers, file_id, failure)

        sub_rule_sql = rule.rule_sql.format(job.submission_id)
        if batch_results:
            # Only run the SQL in batches to save on memory
            proxy = sess.connection().execution_options(stream_results=True).execute(sub_rule_sql)
            while True:
                failures = proxy.fetchmany(SQL_VALIDATION_BATCH_SIZE)
                if not failures:
                    break
                for failure in process_batch(failures, failures[0].keys()):
                    yield failure
            proxy.close()
        else:
            # Run the full SQL and fetch the results
            failures = sess.execute(sub_rule_sql)
            if failures.rowcount:
                for failure in process_batch(failures, failures.keys()):
                    yield failure

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
            'duration': rule_duration,
            'batch_results': batch_results
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
    """ Create a dictionary mapping row numbers of failures to lists of FlexFields

        Args:
            failures: list of failure rows from the SQL validations
            job_id: the current job_id

        Returns:
            a dictionary of row numbers as keys and a list of flex_field objects as the values
    """
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
            expected_value = '{}: {}'.format(fail_header, (str(sql_failure[failure_key] if
                                                               sql_failure[failure_key] is not None else '')))
        # Difference
        elif failure_key == 'difference':
            difference = str(sql_failure[failure_key] if sql_failure[failure_key] is not None else '')
        elif failure_key.startswith(unique_id_start):
            fail_header = failure_key[len(unique_id_start):]
            unique_id_fields.append('{}: {}'.format(fail_header, str(sql_failure[failure_key] if
                                                                     sql_failure[failure_key] is not None else '')))

    # Create strings for fields and values
    values_list = ['{}: {}'.format(header, str(sql_failure[field] if sql_failure[field] is not None else ''))
                   for field, header in zip(cols, col_headers)]
    flex_list = ['{}: {}'.format(flex_field.header, flex_field.cell if flex_field.cell is not None else '')
                 for flex_field in flex_data[row]]
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
