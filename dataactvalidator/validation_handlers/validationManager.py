import csv
import os
import logging
import smart_open
from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.exc import SQLAlchemyError

from dataactbroker.handlers.submission_handler import populate_submission_error_info

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (
    create_file_if_needed, write_file_error, mark_file_complete, run_job_checks, mark_job_status,
    populate_job_error_info, get_action_dates
)

from dataactcore.models.domainModels import matching_cars_subquery
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import FileColumn
from dataactcore.models.stagingModels import DetachedAwardFinancialAssistance, FlexField
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job
from dataactcore.models.validationModels import RuleSql, ValidationLabel

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import get_cross_file_pairs, report_file_name
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

from dataactvalidator.validation_handlers.errorInterface import ErrorInterface
from dataactvalidator.validation_handlers.validator import Validator, cross_validate_sql, validate_file_by_sql
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024


class ValidationManager:
    """ Outer level class, called by flask route """
    reportHeaders = ["Field name", "Error message", "Row number", "Value provided", "Rule label"]
    crossFileReportHeaders = ["Source File", "Target File", "Field names", "Error message", "Values provided",
                              "Row number", "Rule label"]

    def __init__(self, is_local=True, directory=""):
        # Initialize instance variables
        self.is_local = is_local
        self.directory = directory

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.name, FileColumn.name_short, FileColumn.file_id).all()

        self.long_to_short_dict = {}
        self.short_to_long_dict = {}
        # fill in long_to_short and short_to_long dicts
        for col in colnames:
            # Get long_to_short_dict filled in
            if not self.long_to_short_dict.get(col.file_id):
                self.long_to_short_dict[col.file_id] = {}
            self.long_to_short_dict[col.file_id][col.name] = col.name_short

            # Get short_to_long_dict filled in
            if not self.short_to_long_dict.get(col.file_id):
                self.short_to_long_dict[col.file_id] = {}
            self.short_to_long_dict[col.file_id][col.name_short] = col.name

    def get_writer(self, region_name, bucket_name, file_name, header):
        """ Gets the write type based on if its a local install or not.

        Args:
            region_name - AWS region to write to, not used for local
            bucket_name - AWS bucket to write to, not used for local
            file_name - File to be written
            header - Column headers for file to be written
        """
        if self.is_local:
            return CsvLocalWriter(file_name, header)
        return CsvS3Writer(region_name, bucket_name, file_name, header)

    def get_file_name(self, path):
        """ Return full path of error report based on provided name """
        if self.is_local:
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
            record = FieldCleaner.clean_row(next_record, self.long_to_short_dict[job.file_type_id], fields)
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
                writer.writerow(["Formatting Error", ValidationError.readErrorMsg, str(row_number), ""])
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
        error_list = ErrorInterface()
        job_id = job.job_id
        submission_id = job.submission_id

        row_number = 1
        file_type = job.file_type.name
        validation_start = datetime.now()

        log_str = 'on submission_id: {}, job_id: {}, file_type: {}'.format(str(submission_id), str(job_id), file_type)
        logger.info({
            'message': 'Beginning run_validation {}'.format(log_str),
            'message_type': 'ValidatorInfo',
            'submission_id': submission_id,
            'job_id': job_id,
            'file_type': file_type,
            'action': 'run_validations',
            'status': 'start',
            'start_time': validation_start
        })
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
        if self.is_local and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Get bucket name and file name
        file_name = job.filename
        bucket_name = CONFIG_BROKER['aws_bucket']
        region_name = CONFIG_BROKER['aws_region']

        error_file_name = report_file_name(job.submission_id, False, job.file_type.name)
        error_file_path = "".join([CONFIG_SERVICES['error_report_path'], error_file_name])
        warning_file_name = report_file_name(job.submission_id, True, job.file_type.name)
        warning_file_path = "".join([CONFIG_SERVICES['error_report_path'], warning_file_name])

        # Create File Status object
        create_file_if_needed(job_id, file_name)

        reader = CsvReader()

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

        try:
            extension = os.path.splitext(file_name)[1]
            if not extension or extension.lower() not in ['.csv', '.txt']:
                raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.fileTypeError)

            # Count file rows: throws a File Level Error for non-UTF8 characters
            temp_file = open(reader.get_filename(region_name, bucket_name, file_name), encoding='utf-8')
            file_row_count = len(list(csv.reader(temp_file)))
            try:
                temp_file.close()
            except AttributeError:
                # File does not exist, and so does not need to be closed
                pass

            # Pull file and return info on whether it's using short or long col headers
            reader.open_file(region_name, bucket_name, file_name, fields, bucket_name,
                             self.get_file_name(error_file_name), self.long_to_short_dict[job.file_type_id],
                             is_local=self.is_local)

            # list to keep track of rows that fail validations
            error_rows = []

            # While not done, pull one row and put it into staging table if it passes
            # the Validator

            loading_start = datetime.now()
            logger.info({
                'message': 'Beginning data loading {}'.format(log_str),
                'message_type': 'ValidatorInfo',
                'submission_id': submission_id,
                'job_id': job_id,
                'file_type': file_type,
                'action': 'data_loading',
                'status': 'start',
                'start_time': loading_start
            })

            with open(error_file_path, 'w', newline='') as error_file,\
                    open(warning_file_path, 'w', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                required_list = None
                type_list = None
                if file_type == "fabs":
                    # create a list of all required/type labels for FABS
                    labels = sess.query(ValidationLabel).all()
                    required_list = {}
                    type_list = {}
                    for label in labels:
                        if label.label_type == "requirement":
                            required_list[label.column_name] = label.label
                        else:
                            type_list[label.column_name] = label.label

                # write headers to file
                error_csv.writerow(self.reportHeaders)
                warning_csv.writerow(self.reportHeaders)
                while not reader.is_finished:
                    row_number += 1

                    if row_number % 100 == 0:
                        elapsed_time = (datetime.now()-loading_start).total_seconds()
                        logger.info({
                            'message': 'Loading row: {} {}'.format(str(row_number), log_str),
                            'message_type': 'ValidatorInfo',
                            'submission_id': submission_id,
                            'job_id': job_id,
                            'file_type': file_type,
                            'action': 'data_loading',
                            'status': 'loading',
                            'rows_loaded': row_number,
                            'start_time': loading_start,
                            'elapsed_time': elapsed_time
                        })
                    #
                    # first phase of validations: read record and record a
                    # formatting error if there's a problem
                    #
                    (record, reduceRow, skip_row, doneReading, rowErrorHere, flex_cols) = \
                        self.read_record(reader, error_csv, row_number, job, fields, error_list)
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
                        if file_type == "fabs":
                            record['afa_generated_unique'] = (record['award_modification_amendme'] or '-none-') + "_" +\
                                                             (record['awarding_sub_tier_agency_c'] or '-none-') + \
                                                             "_" + (record['fain'] or '-none-') + "_" + \
                                                             (record['uri'] or '-none-')
                        passed_validations, failures, valid = Validator.validate(record, csv_schema,
                                                                                 file_type == "fabs",
                                                                                 required_list, type_list)
                    if valid:
                        # todo: update this logic later when we have actual validations
                        if file_type == "fabs":
                            record["is_valid"] = True

                        model_instance = model(job_id=job_id, submission_id=submission_id,
                                               valid_record=passed_validations, **record)
                        skip_row = not insert_staging_model(model_instance, job, error_csv, error_list)
                        if flex_cols:
                            sess.add_all(flex_cols)
                            sess.commit()

                        if skip_row:
                            error_rows.append(row_number)
                            continue

                    if not passed_validations:
                        fatal = write_errors(failures, job, self.short_to_long_dict[job.file_type_id], error_csv,
                                             warning_csv, row_number, error_list, flex_cols)
                        if fatal:
                            error_rows.append(row_number)

                loading_duration = (datetime.now()-loading_start).total_seconds()
                logger.info({
                    'message': 'Completed data loading {}'.format(log_str),
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
                sql_error_rows = self.run_sql_validations(job, file_type, self.short_to_long_dict[job.file_type_id],
                                                          error_csv, warning_csv, row_number, error_list)
                error_rows.extend(sql_error_rows)
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                # stream error file
                with open(error_file_path, 'rb') as csv_file:
                    with smart_open.smart_open(S3Handler.create_file_path(self.get_file_name(error_file_name)), 'w')\
                            as writer:
                        while True:
                            chunk = csv_file.read(CHUNK_SIZE)
                            if chunk:
                                writer.write(chunk)
                            else:
                                break
                csv_file.close()
                os.remove(error_file_path)

                # stream warning file
                with open(warning_file_path, 'rb') as warning_csv_file:
                    with smart_open.smart_open(S3Handler.create_file_path(self.get_file_name(warning_file_name)), 'w')\
                            as warning_writer:
                        while True:
                            chunk = warning_csv_file.read(CHUNK_SIZE)
                            if chunk:
                                warning_writer.write(chunk)
                            else:
                                break
                warning_csv_file.close()
                os.remove(warning_file_path)

            # Calculate total number of rows in file
            # that passed validations
            error_rows_unique = set(error_rows)
            total_rows_excluding_header = row_number - 1
            valid_rows = total_rows_excluding_header - len(error_rows_unique)

            # Update fabs is_valid rows where applicable
            # Update submission to include action dates where applicable
            if file_type == "fabs":
                sess.query(DetachedAwardFinancialAssistance).\
                    filter(DetachedAwardFinancialAssistance.row_number.in_(error_rows_unique),
                           DetachedAwardFinancialAssistance.submission_id == submission_id).\
                    update({"is_valid": False}, synchronize_session=False)
                sess.commit()
                min_action_date, max_action_date = get_action_dates(submission_id)
                sess.query(Submission).filter(Submission.submission_id == submission_id).\
                    update({"reporting_start_date": min_action_date, "reporting_end_date": max_action_date},
                           synchronize_session=False)

            # Ensure validated rows match initial row count
            if file_row_count != row_number:
                raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.rowCountError)

            # Update job metadata
            job.number_of_rows = row_number
            job.number_of_rows_valid = valid_rows
            sess.commit()

            error_list.write_all_row_errors(job_id)
            # Update error info for submission
            populate_job_error_info(job)

            if file_type == "fabs":
                # set number of errors and warnings for detached submission
                populate_submission_error_info(submission_id)

            # Mark validation as finished in job tracker
            mark_job_status(job_id, "finished")
            mark_file_complete(job_id, file_name)
        except Exception as e:
            logger.error("An exception occurred during validation:{}".format(str(e)))
            raise
        finally:
            # Ensure the files always close
            reader.close()

            validation_duration = (datetime.now()-validation_start).total_seconds()
            logger.info({
                'message': 'Completed run_validation {}'.format(log_str),
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
        sql_failures = validate_file_by_sql(job, file_type, self.short_to_long_dict[job.file_type_id])
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
                writer.writerow([field_name, error_msg, str(failure.row), failure.failed_value, failure.original_label])
            elif failure.severity_id == RULE_SEVERITY_DICT['warning']:
                # write to warnings file
                warning_writer.writerow([field_name, error_msg, str(failure.row), failure.failed_value,
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
        job_start = datetime.now()
        logger.info({
            'message': 'Beginning cross-file validations on submission_id: ' + str(submission_id),
            'message_type': 'ValidatorInfo',
            'submission_id': submission_id,
            'job_id': job.job_id,
            'action': 'run_cross_validations',
            'start': job_start,
            'status': 'start'
        })
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

            # get error file name/path
            error_file_name = report_file_name(submission_id, False, first_file.name, second_file.name)
            error_file_path = "".join([CONFIG_SERVICES['error_report_path'], error_file_name])
            warning_file_name = report_file_name(submission_id, True, first_file.name, second_file.name)
            warning_file_path = "".join([CONFIG_SERVICES['error_report_path'], warning_file_name])

            # open error report and gather failed rules within it
            with open(error_file_path, 'w', newline='') as error_file,\
                    open(warning_file_path, 'w', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # write headers to file
                error_csv.writerow(self.crossFileReportHeaders)
                warning_csv.writerow(self.crossFileReportHeaders)

                # send comboRules to validator.crossValidate sql
                current_cols_short_to_long = self.short_to_long_dict[first_file.id].copy()
                current_cols_short_to_long.update(self.short_to_long_dict[second_file.id].copy())
                cross_validate_sql(combo_rules.all(), submission_id, current_cols_short_to_long, first_file.id,
                                   second_file.id, job, error_csv, warning_csv, error_list, job_id)
            # close files
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                # stream error file
                with open(error_file_path, 'rb') as csv_file:
                    with smart_open.smart_open(S3Handler.create_file_path(self.get_file_name(error_file_name)),
                                               'w') as writer:
                        while True:
                            chunk = csv_file.read(CHUNK_SIZE)
                            if chunk:
                                writer.write(chunk)
                            else:
                                break
                csv_file.close()
                os.remove(error_file_path)

                # stream warning file
                with open(warning_file_path, 'rb') as warning_csv_file:
                    with smart_open.smart_open(S3Handler.create_file_path(self.get_file_name(warning_file_name)),
                                               'w') as warning_writer:
                        while True:
                            chunk = warning_csv_file.read(CHUNK_SIZE)
                            if chunk:
                                warning_writer.write(chunk)
                            else:
                                break
                warning_csv_file.close()
                os.remove(warning_file_path)

        # write all recorded errors to database
        error_list.write_all_row_errors(job_id)
        # Update error info for submission
        populate_job_error_info(job)

        # mark job status as "finished"
        mark_job_status(job_id, "finished")
        job_duration = (datetime.now()-job_start).total_seconds()
        logger.info({
            'message': 'Completed cross-file validations on submission_id: ' + str(submission_id),
            'message_type': 'ValidatorInfo',
            'submission_id': submission_id,
            'job_id': job.job_id,
            'action': 'run_cross_validations',
            'status': 'finish',
            'start': job_start,
            'duration': job_duration
        })
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
        writer.writerow(["Formatting Error", ValidationError.writeErrorMsg, model.row_number, ""])
        error_list.record_row_error(job.job_id, job.filename, "Formatting Error", ValidationError.writeError,
                                    model.row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
        return False
    return True


def write_errors(failures, job, short_colnames, writer, warning_writer, row_number, error_list, flex_cols):
    """ Write errors to error database

    Args:
        failures: List of Failures to be written
        job: Current job
        short_colnames: Dict mapping short names to long names
        writer: CsvWriter object
        warning_writer: CsvWriter object
        row_number: Current row number
        error_list: instance of ErrorInterface to keep track of errors
        flex_cols: all flex columns for this row
    Returns:
        True if any fatal errors were found, False if only warnings are present
    """
    fatal_error_found = False
    # prepare flex cols for all the errors for this row
    flex_col_headers = []
    flex_col_cells = []
    if flex_cols:
        for flex_col in flex_cols:
            flex_col_headers.append(flex_col.header)
            flex_val = flex_col.cell if flex_col.cell else ""
            flex_col_cells.append(flex_col.header + ": " + flex_val)
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
        # get flex fields
        field_names = [field_name]
        flex_list = []
        # only add the value if there's something to add, otherwise our join will look bad
        if failure.value:
            flex_list = [field_name + ": " + failure.value]

        # append whatever list we made of flex columns to our existing field names and content list
        field_names.extend(flex_col_headers)
        flex_list.extend(flex_col_cells)

        # join the field names and flex column values so we have a list instead of a single value
        combined_field_names = ", ".join(field_names)
        fail_value = ", ".join(flex_list)
        if failure.severity == 'fatal':
            fatal_error_found = True
            writer.writerow([combined_field_names, error_msg, str(row_number), fail_value, failure.label])
        elif failure.severity == 'warning':
            # write to warnings file
            warning_writer.writerow([combined_field_names, error_msg, str(row_number), fail_value, failure.label])
        error_list.record_row_error(job.job_id, job.filename, combined_field_names, failure.description, row_number,
                                    failure.label, severity_id=severity_id)
    return fatal_error_found
