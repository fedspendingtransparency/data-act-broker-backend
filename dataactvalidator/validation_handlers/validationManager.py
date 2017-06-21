import os
import logging
from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.exc import SQLAlchemyError

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import matching_cars_subquery
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import FileColumn
from dataactcore.models.stagingModels import DetachedAwardFinancialAssistance, FlexField
from dataactcore.interfaces.function_bag import (
    create_file_if_needed, write_file_error, mark_file_complete, run_job_checks,
    mark_job_status, populate_submission_error_info, populate_job_error_info
)
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import get_cross_file_pairs, report_file_name
from dataactcore.utils.statusCode import StatusCode
from dataactcore.aws.s3Handler import S3Handler
from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.validation_handlers.errorInterface import ErrorInterface
from dataactvalidator.validation_handlers.validator import Validator, cross_validate_sql, validate_file_by_sql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.models.validationModels import RuleSql


logger = logging.getLogger(__name__)


class ValidationManager:
    """
    Outer level class, called by flask route
    """
    reportHeaders = ["Field name", "Error message", "Row number", "Value provided", "Rule label"]
    crossFileReportHeaders = ["Source File", "Target File", "Field names", "Error message", "Values provided",
                              "Row number", "Rule label"]

    def __init__(self, is_local=True, directory=""):
        # Initialize instance variables
        self.isLocal = is_local
        self.directory = directory

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.name, FileColumn.name_short).all()
        self.long_to_short_dict = {row.name: row.name_short for row in colnames}
        self.short_to_long_dict = {row.name_short: row.name for row in colnames}

    def get_reader(self):
        """
        Gets the reader type based on if its local install or not.
        """
        return CsvReader()

    def get_writer(self, region_name, bucket_name, file_name, header):
        """ Gets the write type based on if its a local install or not.

        Args:
            region_name - AWS region to write to, not used for local
            bucket_name - AWS bucket to write to, not used for local
            file_name - File to be written
            header - Column headers for file to be written
        """
        if self.isLocal:
            return CsvLocalWriter(file_name, header)
        return CsvS3Writer(region_name, bucket_name, file_name, header)

    def get_file_name(self, path):
        """ Return full path of error report based on provided name """
        if self.isLocal:
            return os.path.join(self.directory, path)
        # Forcing forward slash here instead of using os.path to write a valid path for S3
        return "".join(["errors/", path])

    def read_record(self, reader, writer, row_number, job, fields, error_list):
        """ Read and process the next record

        Args:
            reader: CsvReader object
            writer: CsvWriter object
            row_number: Next row number to be read
            job: current job
            fields: List of FileColumn objects for this file type
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            Tuple with six elements:
            1. Dict of record after preprocessing
            2. Boolean indicating whether to reduce row count
            3. Boolean indicating whether to skip row
            4. Boolean indicating whether to stop reading
            5. Row error has been found
            6. Dict of flex columns
        """
        reduce_row = False
        row_error_found = False
        job_id = job.job_id
        try:
            (next_record, flex_fields) = reader.get_next_record()
            record = FieldCleaner.clean_row(next_record, self.long_to_short_dict, fields)
            record["row_number"] = row_number
            for flex_field in flex_fields:
                flex_field.submission_id = job.submission_id
                flex_field.job_id = job.job_id
                flex_field.row_number = row_number
                flex_field.file_type_id = job.file_type_id

            if reader.is_finished and len(record) < 2:
                # This is the last line and is empty, don't record an error
                return {}, True, True, True, False, []  # Don't count this row
        except ResponseException:
            if reader.is_finished and reader.extra_line:
                # Last line may be blank don't record an error,
                # reader.extra_line indicates a case where the last valid line has extra line breaks
                # Don't count last row if empty
                reduce_row = True
            else:
                writer.write(["Formatting Error", ValidationError.readErrorMsg, str(row_number), ""])
                error_list.record_row_error(job_id, job.filename, "Formatting Error", ValidationError.readError,
                                            row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
                row_error_found = True

            return {}, reduce_row, True, False, row_error_found, []
        return record, reduce_row, False, False, row_error_found, flex_fields

    def run_validation(self, job):
        """ Run validations for specified job
        Args:
            job: Job to be validated
        Returns:
            True if successful
        """

        sess = GlobalDB.db().session
        job_id = job.job_id

        error_list = ErrorInterface()

        submission_id = job.submission_id

        row_number = 1
        file_type = job.file_type.name
        validation_start = datetime.now()

        logger.info(
            {
                'message': 'Beginning run_validation on submission_id: ' + str(submission_id) +
                ', job_id: ' + str(job_id) + ', file_type: ' + file_type,
                'message_type': 'ValidatorInfo',
                'submission_id': submission_id,
                'job_id': job_id,
                'file_type': file_type,
                'action': 'run_validations',
                'status': 'start',
                'start_time': validation_start})
        # Get orm model for this file
        model = [ft.model for ft in FILE_TYPE if ft.name == file_type][0]

        # Delete existing file level errors for this submission
        sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == job_id).delete()
        sess.commit()

        # Clear existing records for this submission
        sess.query(model).filter_by(submission_id=submission_id).delete()
        sess.commit()

        # Clear existing flex fields for this job
        sess.query(FlexField).filter_by(job_id=job_id).delete()
        sess.commit()

        # If local, make the error report directory
        if self.isLocal and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Get bucket name and file name
        file_name = job.filename
        bucket_name = CONFIG_BROKER['aws_bucket']
        region_name = CONFIG_BROKER['aws_region']

        error_file_name = self.get_file_name(report_file_name(job.submission_id, False, job.file_type.name))
        warning_file_name = self.get_file_name(report_file_name(job.submission_id, True, job.file_type.name))

        # Create File Status object
        create_file_if_needed(job_id, file_name)

        reader = self.get_reader()

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            file_size = S3Handler.get_file_size(file_name)
        else:
            file_size = os.path.getsize(file_name)
        job.file_size = file_size
        sess.commit()

        # Get fields for this file
        fields = sess.query(FileColumn).filter(FileColumn.file_id == FILE_TYPE_DICT[file_type]).all()

        for field in fields:
            sess.expunge(field)

        csv_schema = {row.name_short: row for row in fields}
        fields_long_names = [row.name for row in fields]
        long_to_short_file_type = {long_name: short_name for long_name, short_name in self.long_to_short_dict.items()
                                   if long_name in fields_long_names}

        try:
            # Pull file and return info on whether it's using short or long col headers
            reader.open_file(region_name, bucket_name, file_name, fields, bucket_name, error_file_name,
                             long_to_short_file_type)

            # list to keep track of rows that fail validations
            error_rows = []

            # While not done, pull one row and put it into staging table if it passes
            # the Validator

            loading_start = datetime.now()
            logger.info(
                {
                    'message': 'Beginning data loading on submission_id: ' + str(submission_id) +
                    ', job_id: ' + str(job_id) + ', file_type: ' + file_type,
                    'message_type': 'ValidatorInfo',
                    'submission_id': submission_id,
                    'job_id': job_id,
                    'file_type': file_type,
                    'action': 'data_loading',
                    'status': 'start',
                    'start_time': loading_start})

            with self.get_writer(region_name, bucket_name, error_file_name, self.reportHeaders) as writer, \
                    self.get_writer(region_name, bucket_name, warning_file_name, self.reportHeaders) as warning_writer:
                while not reader.is_finished:
                    row_number += 1

                    if row_number % 100 == 0:

                        elapsed_time = (datetime.now()-loading_start).total_seconds()
                        logger.info(
                            {
                                'message': 'Loading row: ' + str(row_number) + ' on submission_id: ' +
                                str(submission_id) + ', job_id: ' + str(job_id) + ', file_type: ' + file_type,
                                'message_type': 'ValidatorInfo',
                                'submission_id': submission_id,
                                'job_id': job_id,
                                'file_type': file_type,
                                'action': 'data_loading',
                                'status': 'loading',
                                'rows_loaded': row_number,
                                'start_time': loading_start,
                                'elapsed_time': elapsed_time})
                    #
                    # first phase of validations: read record and record a
                    # formatting error if there's a problem
                    #
                    (record, reduceRow, skip_row, doneReading, rowErrorHere, flex_cols) = \
                        self.read_record(reader, writer, row_number, job, fields, error_list)
                    if reduceRow:
                        row_number -= 1
                    if rowErrorHere:
                        error_rows.append(row_number)
                    if doneReading:
                        # Stop reading from input file
                        break
                    elif skip_row:
                        # Do not write this row to staging, but continue processing future rows
                        continue

                    #
                    # second phase of validations: do basic schema checks
                    # (e.g., require fields, field length, data type)
                    #
                    # D files are obtained from upstream systems (ASP and FPDS) that perform their own basic
                    # validations, so these validations are not repeated here
                    if file_type in ["award", "award_procurement"]:
                        # Skip basic validations for D files, set as valid to trigger write to staging
                        passed_validations = True
                        valid = True
                    else:
                        passed_validations, failures, valid = Validator.validate(record, csv_schema)
                    if valid:
                        # todo: update this logic later when we have actual validations
                        if file_type in ["detached_award"]:
                            record["is_valid"] = True

                        model_instance = model(job_id=job_id, submission_id=submission_id,
                                               valid_record=passed_validations, **record)
                        skip_row = not insert_staging_model(model_instance, job, writer, error_list)
                        if flex_cols:
                            sess.add_all(flex_cols)
                            sess.commit()

                        if skip_row:
                            error_rows.append(row_number)
                            continue

                    if not passed_validations:
                        fatal = write_errors(failures, job, self.short_to_long_dict, writer, warning_writer,
                                             row_number, error_list)
                        if fatal:
                            error_rows.append(row_number)

                loading_duration = (datetime.now()-loading_start).total_seconds()
                logger.info(
                    {
                        'message': 'Completed data loading on submission_id: ' + str(submission_id) +
                        ', job_id: ' + str(job_id) + ', file_type: ' + file_type,
                        'message_type': 'ValidatorInfo',
                        'submission_id': submission_id,
                        'job_id': job_id,
                        'file_type': file_type,
                        'action': 'data_loading',
                        'status': 'finish',
                        'start_time': loading_start,
                        'end_time': datetime.now(),
                        'duration': loading_duration,
                        'total_rows': row_number
                    })

                if file_type in ('appropriations', 'program_activity', 'award_financial'):
                    update_tas_ids(model, submission_id)
                #
                # third phase of validations: run validation rules as specified
                # in the schema guidance. these validations are sql-based.
                #
                sql_error_rows = self.run_sql_validations(job, file_type, self.short_to_long_dict, writer,
                                                          warning_writer, row_number, error_list)
                error_rows.extend(sql_error_rows)

                # Write unfinished batch
                writer.finish_batch()
                warning_writer.finish_batch()

            # Calculate total number of rows in file
            # that passed validations
            error_rows_unique = set(error_rows)
            total_rows_excluding_header = row_number - 1
            valid_rows = total_rows_excluding_header - len(error_rows_unique)

            # Update detached_award is_valid rows where applicable
            if file_type in ["detached_award"]:
                sess.query(DetachedAwardFinancialAssistance).\
                    filter(DetachedAwardFinancialAssistance.row_number.in_(error_rows_unique),
                           DetachedAwardFinancialAssistance.submission_id == submission_id).\
                    update({"is_valid": False}, synchronize_session=False)

            # Update job metadata
            job.number_of_rows = row_number
            job.number_of_rows_valid = valid_rows
            sess.commit()

            error_list.write_all_row_errors(job_id)
            # Update error info for submission
            populate_job_error_info(job)

            # Mark validation as finished in job tracker
            mark_job_status(job_id, "finished")
            mark_file_complete(job_id, file_name)
        finally:
            # Ensure the file always closes
            reader.close()

            validation_duration = (datetime.now()-validation_start).total_seconds()
            logger.info(
                {
                    'message': 'Completed run_validation on submission_id: ' + str(submission_id) +
                    ', job_id: ' + str(job_id) + ', file_type: ' + file_type,
                    'message_type': 'ValidatorInfo',
                    'submission_id': submission_id,
                    'job_id': job_id,
                    'file_type': file_type,
                    'action': 'run_validation',
                    'status': 'finish',
                    'start_time': validation_start,
                    'end_time': datetime.now(),
                    'duration': validation_duration
                })

        return True

    def run_sql_validations(self, job, file_type, short_colnames, writer, warning_writer, row_number, error_list):
        """ Run all SQL rules for this file type

        Args:
            job: Current job
            file_type: Type of file for current job
            short_colnames: Dict mapping short field names to long
            writer: CsvWriter object
            warning_writer: CsvWriter for warnings
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        job_id = job.job_id
        error_rows = []
        sql_failures = validate_file_by_sql(job, file_type, self.short_to_long_dict)
        for failure in sql_failures:
            # convert shorter, machine friendly column names used in the
            # SQL validation queries back to their long names
            if failure.field_name in short_colnames:
                field_name = short_colnames[failure.field_name]
            else:
                field_name = failure.field_name

            if failure.severity_id == RULE_SEVERITY_DICT['fatal']:
                error_rows.append(failure.row)

            try:
                # If error is an int, it's one of our prestored messages
                error_type = int(failure.error)
                error_msg = ValidationError.get_error_message(error_type)
            except ValueError:
                # If not, treat it literally
                error_msg = failure.error

            if failure.severity_id == RULE_SEVERITY_DICT['fatal']:
                writer.write([field_name, error_msg, str(failure.row), failure.failed_value, failure.original_label])
            elif failure.severity_id == RULE_SEVERITY_DICT['warning']:
                # write to warnings file
                warning_writer.write([field_name, error_msg, str(failure.row), failure.failed_value,
                                      failure.original_label])
            error_list.record_row_error(job_id, job.filename, field_name, failure.error, row_number,
                                        failure.original_label, failure.file_type_id, failure.target_file_id,
                                        failure.severity_id)
        return error_rows

    def run_cross_validation(self, job):
        """ Cross file validation job. Test all rules with matching rule_timing.
            Run each cross-file rule and create error report.

            Args:
                job: Current job
        """
        sess = GlobalDB.db().session
        job_id = job.job_id
        # Create File Status object
        create_file_if_needed(job_id)
        # Create list of errors
        error_list = ErrorInterface()

        submission_id = job.submission_id
        bucket_name = CONFIG_BROKER['aws_bucket']
        region_name = CONFIG_BROKER['aws_region']
        job_start = datetime.now()
        logger.info(
            {
                'message': 'Beginning cross-file validations on submission_id: ' + str(submission_id),
                'message_type': 'ValidatorInfo',
                'submission_id': submission_id,
                'job_id': job.job_id,
                'action': 'run_cross_validations',
                'start': job_start,
                'status': 'start'})
        # Delete existing cross file errors for this submission
        sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == job_id).delete()
        sess.commit()

        # get all cross file rules from db
        cross_file_rules = sess.query(RuleSql).filter_by(rule_cross_file_flag=True)

        # for each cross-file combo, run associated rules and create error report
        for c in get_cross_file_pairs():
            first_file = c[0]
            second_file = c[1]
            combo_rules = cross_file_rules.filter(or_(and_(
                RuleSql.file_id == first_file.id,
                RuleSql.target_file_id == second_file.id), and_(
                RuleSql.file_id == second_file.id,
                RuleSql.target_file_id == first_file.id)))
            # send comboRules to validator.crossValidate sql
            failures = cross_validate_sql(combo_rules.all(), submission_id, self.short_to_long_dict, first_file.id,
                                          second_file.id, job)
            # get error file name
            report_filename = self.get_file_name(report_file_name(submission_id, False, first_file.name,
                                                                  second_file.name))
            warning_report_filename = self.get_file_name(report_file_name(submission_id, True, first_file.name,
                                                                          second_file.name))

            # loop through failures to create the error report
            with self.get_writer(region_name, bucket_name, report_filename, self.crossFileReportHeaders) as writer, \
                    self.get_writer(region_name, bucket_name, warning_report_filename, self.crossFileReportHeaders) as \
                    warning_writer:
                for failure in failures:
                    if failure[9] == RULE_SEVERITY_DICT['fatal']:
                        writer.write(failure[0:7])
                    if failure[9] == RULE_SEVERITY_DICT['warning']:
                        warning_writer.write(failure[0:7])
                    error_list.record_row_error(job_id, "cross_file",
                                                failure[0], failure[3], failure[5], failure[6],
                                                failure[7], failure[8], severity_id=failure[9])
                # write the last unfinished batch
                writer.finish_batch()
                warning_writer.finish_batch()

        # write all recorded errors to database
        error_list.write_all_row_errors(job_id)
        # mark job status as "finished"
        mark_job_status(job_id, "finished")
        job_duration = (datetime.now()-job_start).total_seconds()
        logger.info(
            {
                'message': 'Completed cross-file validations on submission_id: ' + str(submission_id),
                'message_type': 'ValidatorInfo',
                'submission_id': submission_id,
                'job_id': job.job_id,
                'action': 'run_cross_validations',
                'status': 'finish',
                'start': job_start,
                'duration': job_duration})
        # set number of errors and warnings for submission.
        submission = populate_submission_error_info(submission_id)
        # TODO: Remove temporary step below
        # Temporarily set publishable flag at end of cross file, remove this once users are able to mark their
        # submissions as publishable
        # Publish only if no errors are present
        if submission.number_of_errors == 0:
            submission.publishable = True
        sess.commit()

        # Mark validation complete
        mark_file_complete(job_id)

    def validate_job(self, job_id):
        """ Gets file for job, validates each row, and sends valid rows to a staging table
        Args:
        request -- HTTP request containing the jobId
        Returns:
        Http response object
        """
        # Create connection to job tracker database
        sess = GlobalDB.db().session

        # Get the job
        job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
        if job is None:
            validation_error_type = ValidationError.jobError
            write_file_error(job_id, None, validation_error_type)
            raise ResponseException('Job ID {} not found in database'.format(job_id), StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Make sure job's prerequisites are complete
        if not run_job_checks(job_id):
            validation_error_type = ValidationError.jobError
            write_file_error(job_id, None, validation_error_type)
            raise ResponseException('Prerequisites for Job ID {} are not complete'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None, validation_error_type)

        # Make sure this is a validation job
        if job.job_type.name in ('csv_record_validation', 'validation'):
            job_type_name = job.job_type.name
        else:
            validation_error_type = ValidationError.jobError
            write_file_error(job_id, None, validation_error_type)
            raise ResponseException(
                'Job ID {} is not a validation job (job type is {})'.format(job_id, job.job_type.name),
                StatusCode.CLIENT_ERROR, None, validation_error_type)

        # set job status to running and do validations
        mark_job_status(job_id, "running")
        if job_type_name == 'csv_record_validation':
            self.run_validation(job)
        elif job_type_name == 'validation':
            self.run_cross_validation(job)
        else:
            raise ResponseException("Bad job type for validator", StatusCode.INTERNAL_ERROR)

        # Update last validated date
        job.last_validated = datetime.utcnow()
        sess.commit()
        return JsonResponse.create(StatusCode.OK, {"message": "Validation complete"})


def update_tas_ids(model_class, submission_id):
    sess = GlobalDB.db().session
    submission = sess.query(Submission).filter_by(submission_id=submission_id).one()

    subquery = matching_cars_subquery(sess, model_class, submission.reporting_start_date, submission.reporting_end_date)
    sess.query(model_class).filter_by(submission_id=submission_id).\
        update({getattr(model_class, 'tas_id'): subquery}, synchronize_session=False)
    sess.commit()


def insert_staging_model(model, job, writer, error_list):
    """ If there is an error during ORM insertion, mark that and continue

    Args:
        model: ORM model instance for the current row
        job: Current job
        writer: CsvWriter object
        error_list: instance of ErrorInterface to keep track of errors

    Returns:
        True if insertion was a success, False otherwise
    """
    sess = GlobalDB.db().session
    try:
        sess.add(model)
        sess.commit()
    except SQLAlchemyError:
        sess.rollback()
        # Write failed, move to next record
        writer.write(["Formatting Error", ValidationError.writeErrorMsg, model.row_number, ""])
        error_list.record_row_error(job.job_id, job.filename, "Formatting Error", ValidationError.writeError,
                                    model.row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
        return False
    return True


def write_errors(failures, job, short_colnames, writer, warning_writer, row_number, error_list):
    """ Write errors to error database

    Args:
        failures: List of Failures to be written
        job: Current job
        short_colnames: Dict mapping short names to long names
        writer: CsvWriter object
        warning_writer: CsvWriter object
        row_number: Current row number
        error_list: instance of ErrorInterface to keep track of errors
    Returns:
        True if any fatal errors were found, False if only warnings are present
    """
    fatal_error_found = False
    # For each failure, record it in error report and metadata
    for failure in failures:
        # map short column names back to long names
        if failure.field in short_colnames:
            field_name = short_colnames[failure.field]
        else:
            field_name = failure.field

        severity_id = RULE_SEVERITY_DICT[failure.severity]
        try:
            # If error is an int, it's one of our prestored messages
            error_type = int(failure.description)
            error_msg = ValidationError.get_error_message(error_type)
        except ValueError:
            # If not, treat it literally
            error_msg = failure.description
        if failure.severity == 'fatal':
            fatal_error_found = True
            writer.write([field_name, error_msg, str(row_number), failure.value, failure.label])
        elif failure.severity == 'warning':
            # write to warnings file
            warning_writer.write([field_name, error_msg, str(row_number), failure.value, failure.label])
        error_list.record_row_error(job.job_id, job.filename, field_name, failure.description, row_number,
                                    failure.label, severity_id=severity_id)
    return fatal_error_found
