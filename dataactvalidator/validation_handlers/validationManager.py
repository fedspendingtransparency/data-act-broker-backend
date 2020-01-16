import boto3
import csv
import logging
import os
import traceback
import pandas as pd

from datetime import datetime

from sqlalchemy import and_, or_

from dataactbroker.handlers.submission_handler import populate_submission_error_info
from dataactbroker.helpers.validation_helper import (
    derive_fabs_awarding_sub_tier, derive_fabs_afa_generated_unique, derive_fabs_unique_award_key, derive_unique_id,
    check_required, check_type, check_length, clean_col, clean_numbers, concat_flex, process_formatting_errors,
    parse_fields, simple_file_scan)

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (
    create_file_if_needed, write_file_error, mark_file_complete, run_job_checks, mark_job_status,
    populate_job_error_info, get_action_dates
)

from dataactcore.models.domainModels import matching_cars_subquery, Office, concat_tas_dict, concat_display_tas_dict
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
from dataactvalidator.scripts.loader_utils import insert_dataframe
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner, StringCleaner

from dataactvalidator.validation_handlers.errorInterface import ErrorInterface
from dataactvalidator.validation_handlers.validator import cross_validate_sql, validate_file_by_sql
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000


class ValidationManager:
    """ Outer level class, called by flask route """
    report_headers = ['Unique ID', 'Field Name', 'Error Message', 'Value Provided', 'Expected Value', 'Difference',
                      'Flex Field', 'Row Number', 'Rule Label']
    cross_file_report_headers = ['Unique ID', 'Source File', 'Source Field Name', 'Target File', 'Target Field Name',
                                 'Error Message', 'Source Value Provided', 'Target Value Provided', 'Difference',
                                 'Source Flex Field', 'Source Row Number', 'Rule Label']

    def __init__(self, is_local=True, directory=""):
        # Initialize instance variables
        self.is_local = is_local
        self.directory = directory
        self.log_str = ''

        # Validation Info
        self.job = None
        self.submission_id = None
        self.file_type = None
        self.file_name = None
        self.is_fabs = False
        self.model = None
        self.error_file_name = None
        self.error_file_path = None
        self.warning_file_name = None
        self.warning_file_path = None

        # Schema info
        self.csv_schema = {}
        self.fields = {}
        self.parsed_fields = {}
        self.expected_headers = []
        self.long_to_short_dict = {}
        self.short_to_long_dict = {}
        self.daims_to_short_dict = {}
        self.short_to_daims_dict = {}

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.daims_name, FileColumn.name, FileColumn.name_short, FileColumn.file_id).all()

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

            # Get daims_to_short_dict filled in
            if not self.daims_to_short_dict.get(col.file_id):
                self.daims_to_short_dict[col.file_id] = {}
            clean_daims = StringCleaner.clean_string(col.daims_name, remove_extras=False)
            self.daims_to_short_dict[col.file_id][clean_daims] = col.name_short

            # Get short_to_daims_dict filled in
            if not self.short_to_daims_dict.get(col.file_id):
                self.short_to_daims_dict[col.file_id] = {}
            self.short_to_daims_dict[col.file_id][col.name_short] = col.daims_name

    def get_file_name(self, path):
        """ Return full path of error report based on provided name """
        if self.is_local:
            return os.path.join(self.directory, path)
        # Forcing forward slash here instead of using os.path to write a valid path for S3
        return "".join(["errors/", path])

    def run_validation(self, job):
        """ Run validations for specified job

            Args:
                job: Job to be validated

            Returns:
                True if successful
        """

        sess = GlobalDB.db().session
        self.job = job
        self.submission_id = job.submission_id
        self.file_type = job.file_type
        self.is_fabs = (self.file_type.name == 'fabs')

        # initializing processing metadata vars for a new validation
        self.reader = CsvReader()
        self.error_list = ErrorInterface()
        self.error_rows = []
        self.max_row_number = 1
        self.total_rows = 0
        self.short_rows = []
        self.long_rows = []

        validation_start = datetime.now()
        bucket_name = CONFIG_BROKER['aws_bucket']
        region_name = CONFIG_BROKER['aws_region']

        self.log_str = 'on submission_id: {}, job_id: {}, file_type: {}'.format(
            str(self.submission_id), str(self.job.job_id), self.file_type.name)
        logger.info({
            'message': 'Beginning run_validation {}'.format(self.log_str),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'run_validations',
            'status': 'start',
            'start_time': validation_start
        })
        # Get orm model for this file
        self.model = [ft.model for ft in FILE_TYPE if ft.name == self.file_type.name][0]

        # Delete existing file level errors for this submission
        sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == self.job.job_id).delete()
        sess.commit()

        # Clear existing records for this submission
        sess.query(self.model).filter_by(submission_id=self.submission_id).delete()
        sess.commit()

        # Clear existing flex fields for this job
        sess.query(FlexField).filter_by(job_id=self.job.job_id).delete()
        sess.commit()

        # If local, make the error report directory
        if self.is_local and not os.path.exists(self.directory):
            os.makedirs(self.directory)

        self.file_name = job.filename
        create_file_if_needed(self.job.job_id, self.file_name)

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            file_size = S3Handler.get_file_size(self.file_name)
        else:
            file_size = os.path.getsize(self.file_name)
        self.job.file_size = file_size
        sess.commit()

        # Get fields for this file
        self.fields = sess.query(FileColumn).filter(FileColumn.file_id == FILE_TYPE_DICT[self.file_type.name])\
            .order_by(FileColumn.daims_name.asc()).all()
        self.expected_headers, self.parsed_fields = parse_fields(sess, self.fields)
        self.csv_schema = {row.name_short: row for row in self.fields}

        try:
            # Loading data and initial validations
            file_row_count = self.load_file_data(sess, bucket_name, region_name)

            if self.file_type.name in ('appropriations', 'program_activity', 'award_financial'):
                update_tas_ids(self.model, self.submission_id)

            # SQL Validations
            with open(self.error_file_path, 'a', newline='') as error_file, \
                    open(self.warning_file_path, 'a', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # third phase of validations: run validation rules as specified in the schema guidance. These
                # validations are sql-based.
                sql_error_rows = self.run_sql_validations(self.short_to_long_dict[self.file_type.file_type_id],
                                                          error_csv, warning_csv)
                self.error_rows.extend(sql_error_rows)
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                s3_resource = boto3.resource('s3', region_name=region_name)
                # stream error file
                with open(self.error_file_path, 'rb') as csv_file:
                    s3_resource.Object(bucket_name, self.get_file_name(self.error_file_name)).put(Body=csv_file)
                csv_file.close()
                os.remove(self.error_file_path)

                # stream warning file
                with open(self.warning_file_path, 'rb') as warning_csv_file:
                    s3_resource.Object(bucket_name,
                                       self.get_file_name(self.warning_file_name)).put(Body=warning_csv_file)
                warning_csv_file.close()
                os.remove(self.warning_file_path)

            # Calculate total number of rows in file that passed validations
            error_rows_unique = set(self.error_rows)
            total_rows_excluding_header = self.total_rows - 1
            valid_rows = total_rows_excluding_header - len(error_rows_unique)

            # Update fabs is_valid rows where applicable
            # Update submission to include action dates where applicable
            if self.is_fabs:
                sess.query(DetachedAwardFinancialAssistance). \
                    filter(DetachedAwardFinancialAssistance.row_number.in_(error_rows_unique),
                           DetachedAwardFinancialAssistance.submission_id == self.submission_id). \
                    update({"is_valid": False}, synchronize_session=False)
                sess.commit()
                min_action_date, max_action_date = get_action_dates(self.submission_id)
                sess.query(Submission).filter(Submission.submission_id == self.submission_id). \
                    update({"reporting_start_date": min_action_date, "reporting_end_date": max_action_date},
                           synchronize_session=False)

            # Ensure validated rows match initial row count
            if file_row_count != self.total_rows:
                raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.rowCountError)

            # Update job metadata
            self.job.number_of_rows = self.total_rows
            self.job.number_of_rows_valid = valid_rows
            sess.commit()

            self.error_list.write_all_row_errors(self.job.job_id)
            # Update error info for submission
            populate_job_error_info(self.job)

            if self.is_fabs:
                # set number of errors and warnings for detached submission
                populate_submission_error_info(self.submission_id)

            # Mark validation as finished in job tracker
            mark_job_status(self.job.job_id, "finished")
            mark_file_complete(self.job.job_id, self.file_name)

        except Exception:
            logger.error({
                'message': 'An exception occurred during validation',
                'message_type': 'ValidatorInfo',
                'submission_id': self.submission_id,
                'job_id': self.job.job_id,
                'file_type': self.file_type.name,
                'traceback': traceback.format_exc()
            })
            raise

        finally:
            # Ensure the files always close
            self.reader.close()

            validation_duration = (datetime.now()-validation_start).total_seconds()
            logger.info({
                'message': 'Completed run_validation {}'.format(self.log_str),
                'message_type': 'ValidatorInfo',
                'submission_id': self.submission_id,
                'job_id': self.job.job_id,
                'file_type': self.file_type.name,
                'action': 'run_validation',
                'status': 'finish',
                'start_time': validation_start,
                'end_time': datetime.now(),
                'duration': validation_duration
            })

        return True

    def load_file_data(self, sess, bucket_name, region_name):
        """ Loads in the file data and performs validations

            Args:
                sess: the database connection

            Returns:
                the number of lines in the file
        """
        loading_start = datetime.now()
        logger.info({
            'message': 'Beginning data loading {}'.format(self.log_str),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'data_loading',
            'status': 'start',
            'start_time': loading_start
        })

        # Extension Check
        extension = os.path.splitext(self.file_name)[1]
        if not extension or extension.lower() not in ['.csv', '.txt']:
            raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.fileTypeError)

        # Base file check
        file_row_count, self.short_rows, self.long_rows = simple_file_scan(self.reader, bucket_name, region_name,
                                                                           self.file_name)
        # total_rows = header + long_rows (and will be added on per chunk)
        self.total_rows = 1 + len(self.long_rows)

        # Making base error/warning files
        self.error_file_name = report_file_name(self.submission_id, False, self.file_type.name)
        self.error_file_path = "".join([CONFIG_SERVICES['error_report_path'], self.error_file_name])
        self.warning_file_name = report_file_name(self.submission_id, True, self.file_type.name)
        self.warning_file_path = "".join([CONFIG_SERVICES['error_report_path'], self.warning_file_name])

        with open(self.error_file_path, 'w', newline='') as error_file, \
                open(self.warning_file_path, 'w', newline='') as warning_file:
            error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            error_csv.writerow(self.report_headers)
            warning_csv.writerow(self.report_headers)

        # Adding formatting errors to error file
        format_error_df = process_formatting_errors(self.short_rows, self.long_rows, self.report_headers)
        format_error_df.to_csv(self.error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                               mode='a', header=False)

        # Finally open the file for loading into the database with baseline validations
        self.reader.open_file(region_name, bucket_name, self.file_name, self.fields, bucket_name,
                              self.get_file_name(self.error_file_name),
                              self.daims_to_short_dict[self.file_type.file_type_id],
                              self.short_to_daims_dict[self.file_type.file_type_id],
                              is_local=self.is_local)
        self.reader.file.seek(0)
        reader_obj = pd.read_csv(self.reader.file, dtype=str, delimiter=self.reader.delimiter, error_bad_lines=False,
                                 keep_default_na=False, chunksize=CHUNK_SIZE, warn_bad_lines=False)
        for chunk_df in reader_obj:
            self.process_data_chunk(sess, chunk_df)

        loading_duration = (datetime.now() - loading_start).total_seconds()
        logger.info({
            'message': 'Completed data loading {}'.format(self.log_str),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'data_loading',
            'status': 'finish',
            'start_time': loading_start,
            'end_time': datetime.now(),
            'duration': loading_duration,
            'total_rows': self.total_rows
        })

        return file_row_count

    def process_data_chunk(self, sess, chunk_df):
        logger.info({
            'message': 'Loading rows starting from {}'.format(self.max_row_number + 1),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'data_loading',
            'status': 'start'
        })

        # TODO: Move all this dataframe stuff to some reasonable location
        # Replace whatever the user included so we're using the database headers
        chunk_df.rename(columns=self.reader.header_dict, inplace=True)

        empty_file = chunk_df.empty

        if not empty_file:
            chunk_df = chunk_df.applymap(clean_col)

            # Adding row number
            chunk_df = chunk_df.reset_index()
            chunk_df['row_number'] = chunk_df.index + 1 + self.max_row_number
            self.total_rows += len(chunk_df.index)

            # Increment row numbers if any were ignored being too long
            for row in sorted(self.long_rows):
                chunk_df.loc[chunk_df['row_number'] >= row, 'row_number'] = chunk_df['row_number'] + 1

            # Setting max row number for chunking purposes
            self.max_row_number = chunk_df['row_number'].max()

            self.long_rows = [row for row in self.long_rows if row > self.max_row_number]

            # Drop rows that were too short and pandas filled in with Nones
            chunk_df = chunk_df[~chunk_df['row_number'].isin(self.short_rows)]

            chunk_df = chunk_df.drop(['index'], axis=1)

            # Drop all rows that have 1 or less filled in values (row_number is always filled in so this is how
            # we have to drop all rows that are just empty)
            chunk_df.dropna(thresh=2, inplace=True)
            empty_file = chunk_df.empty

            flex_data = None
            if self.reader.flex_fields:
                flex_data = chunk_df.loc[:, list(self.reader.flex_fields + ['row_number'])]

            if flex_data is not None and not flex_data.empty:
                flex_data['concatted'] = flex_data.apply(lambda x: concat_flex(x), axis=1)

            chunk_df = chunk_df[list(self.expected_headers + ['row_number'])]

            if not empty_file:
                for field in self.parsed_fields['padded']:
                    chunk_df[field] = chunk_df.apply(
                        lambda x: FieldCleaner.pad_field(self.csv_schema[field], x[field]), axis=1)

        # While not done, pull rows in chunks and put them into staging table if they pass the Validator

        required_list = None
        type_list = None
        office_list = {}
        if not empty_file and self.is_fabs:
            # create a list of all required/type labels for FABS
            labels = sess.query(ValidationLabel).all()
            required_list = {}
            type_list = {}
            for label in labels:
                if label.label_type == "requirement":
                    required_list[label.column_name] = label.label
                else:
                    type_list[label.column_name] = label.label

            # Create a list of all offices
            offices = sess.query(Office.office_code, Office.sub_tier_code).all()
            for office in offices:
                office_list[office.office_code] = office.sub_tier_code

            # Clear out office list to save space
            del offices

        # initializing warning/error files and dataframes
        total_errors = pd.DataFrame(columns=self.report_headers)
        total_warnings = pd.DataFrame(columns=self.report_headers)

        # Only do validations if it's not a D file
        if not empty_file and self.file_type.name not in ['award', 'award_procurement']:
            # Cleaning up numbers so they can be inserted properly
            for field in self.parsed_fields['number']:
                chunk_df[field] = chunk_df.apply(lambda x: clean_numbers(x[field]), axis=1)

            if self.is_fabs:
                chunk_df['is_valid'] = True
                chunk_df['awarding_sub_tier_agency_c'] = chunk_df.apply(
                    lambda x: derive_fabs_awarding_sub_tier(x, office_list), axis=1)
                chunk_df['afa_generated_unique'] = chunk_df.apply(
                    lambda x: derive_fabs_afa_generated_unique(x), axis=1)
                chunk_df['unique_award_key'] = chunk_df.apply(
                    lambda x: derive_fabs_unique_award_key(x), axis=1)
            else:
                chunk_df['tas'] = chunk_df.apply(lambda x: concat_tas_dict(x), axis=1)
                chunk_df['display_tas'] = chunk_df.apply(lambda x: concat_display_tas_dict(x), axis=1)

            chunk_df['unique_id'] = chunk_df.apply(lambda x: derive_unique_id(x, self.is_fabs), axis=1)

            # Separate each of the checks to their own dataframes, then concat them together
            req_errors = check_required(chunk_df, self.parsed_fields['required'], required_list, self.report_headers,
                                        self.short_to_long_dict[self.file_type.file_type_id], flex_data,
                                        is_fabs=self.is_fabs)
            type_errors = check_type(chunk_df, self.parsed_fields['number'] + self.parsed_fields['boolean'], type_list,
                                     self.report_headers, self.csv_schema,
                                     self.short_to_long_dict[self.file_type.file_type_id],
                                     flex_data, is_fabs=self.is_fabs)
            type_error_rows = type_errors['Row Number'].tolist()
            length_errors = check_length(chunk_df, self.parsed_fields['length'], self.report_headers, self.csv_schema,
                                         self.short_to_long_dict[self.file_type.file_type_id], flex_data,
                                         type_error_rows)

            if self.is_fabs:
                error_dfs = [req_errors, type_errors, length_errors]
                warning_dfs = [pd.DataFrame(columns=list(self.report_headers + ['error_type']))]
            else:
                error_dfs = [req_errors, type_errors]
                warning_dfs = [length_errors]

            total_errors = pd.concat(error_dfs, ignore_index=True)
            total_warnings = pd.concat(warning_dfs, ignore_index=True)

            # Converting these to ints because pandas likes to change them to floats randomly
            total_errors[['Row Number', 'error_type']] = total_errors[['Row Number', 'error_type']].astype(int)
            total_warnings[['Row Number', 'error_type']] = total_warnings[['Row Number', 'error_type']]. \
                astype(int)

            self.error_rows.extend([int(x) for x in total_errors['Row Number'].tolist()])

            for index, row in total_errors.iterrows():
                self.error_list.record_row_error(self.job.job_id, self.file_name, row['Field Name'], row['error_type'],
                                                 row['Row Number'], row['Rule Label'], self.file_type.file_type_id,
                                                 None, RULE_SEVERITY_DICT['fatal'])

            for index, row in total_warnings.iterrows():
                self.error_list.record_row_error(self.job.job_id, self.file_name, row['Field Name'], row['error_type'],
                                                 row['Row Number'], row['Rule Label'], self.file_type.file_type_id,
                                                 None, RULE_SEVERITY_DICT['warning'])

            total_errors.drop(['error_type'], axis=1, inplace=True, errors='ignore')
            total_warnings.drop(['error_type'], axis=1, inplace=True, errors='ignore')

            # Remove type error rows from original dataframe
            chunk_df = chunk_df[~chunk_df['row_number'].isin(type_error_rows)]
            chunk_df.drop(['unique_id'], axis=1, inplace=True)

        total_errors.to_csv(self.error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                            mode='a', header=False)
        total_warnings.to_csv(self.warning_file_path, columns=self.report_headers, index=False,
                              quoting=csv.QUOTE_ALL, mode='a', header=False)

        if not empty_file:
            now = datetime.now()
            chunk_df['created_at'] = now
            chunk_df['updated_at'] = now
            chunk_df['job_id'] = self.job.job_id
            chunk_df['submission_id'] = self.submission_id
            insert_dataframe(chunk_df, self.model.__table__.name, sess.connection())

        if not empty_file and flex_data is not None:
            flex_data.drop(['concatted'], axis=1, inplace=True)
            flex_data = flex_data[flex_data['row_number'].isin(chunk_df['row_number'])]

            flex_rows = pd.melt(flex_data, id_vars=['row_number'], value_vars=self.reader.flex_fields,
                                var_name='header', value_name='cell')

            # Filling in all the shared data for these flex fields
            now = datetime.now()
            flex_rows['created_at'] = now
            flex_rows['updated_at'] = now
            flex_rows['job_id'] = self.job.job_id
            flex_rows['submission_id'] = self.submission_id
            flex_rows['file_type_id'] = self.file_type.file_type_id

            # Adding the entire set of flex fields
            insert_dataframe(flex_rows, FlexField.__table__.name, sess.connection())
        sess.commit()

        logger.info({
            'message': 'Loaded rows up to {}'.format(self.max_row_number),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'data_loading',
            'status': 'end'
        })

    def run_sql_validations(self, short_colnames, writer, warning_writer):
        """ Run all SQL rules for this file type

        Args:
            short_colnames: Dict mapping short field names to long
            writer: CsvWriter object for error file
            warning_writer: CsvWriter object for warning file

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        error_rows = []
        sql_failures = validate_file_by_sql(self.job, self.file_type.name,
                                            self.short_to_long_dict[self.file_type.file_type_id])
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
                writer.writerow([failure.unique_id, field_name, error_msg, failure.failed_value, failure.expected_value,
                                 failure.difference, failure.flex_fields, str(failure.row), failure.original_label])
            elif failure.severity_id == RULE_SEVERITY_DICT['warning']:
                # write to warnings file
                warning_writer.writerow([failure.unique_id, field_name, error_msg, failure.failed_value,
                                         failure.expected_value, failure.difference, failure.flex_fields,
                                         str(failure.row), failure.original_label])
            # labeled errors
            self.error_list.record_row_error(self.job.job_id, self.file_name, field_name, failure.error,
                                             self.total_rows, failure.original_label, failure.file_type_id,
                                             failure.target_file_id, failure.severity_id)
        return error_rows

    def run_cross_validation(self, job):
        """ Cross file validation job. Test all rules with matching rule_timing. Run each cross-file rule and create
            error report.

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
                error_csv.writerow(self.cross_file_report_headers)
                warning_csv.writerow(self.cross_file_report_headers)

                # send comboRules to validator.crossValidate sql
                current_cols_short_to_long = self.short_to_long_dict[first_file.id].copy()
                current_cols_short_to_long.update(self.short_to_long_dict[second_file.id].copy())
                cross_validate_sql(combo_rules.all(), submission_id, current_cols_short_to_long, job_id, error_csv,
                                   warning_csv, error_list)
            # close files
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                s3_resource = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
                # stream error file
                with open(error_file_path, 'rb') as csv_file:
                    s3_resource.Object(CONFIG_BROKER['aws_bucket'], self.get_file_name(error_file_name)).\
                        put(Body=csv_file)
                csv_file.close()
                os.remove(error_file_path)

                # stream warning file
                with open(warning_file_path, 'rb') as warning_csv_file:
                    s3_resource.Object(CONFIG_BROKER['aws_bucket'], self.get_file_name(warning_file_name)).\
                        put(Body=warning_csv_file)
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
                job_id: Database ID for the validation Job

            Returns:
                Http response object
        """
        # Create connection to job tracker database
        sess = GlobalDB.db().session

        # Get the job
        job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
        if job is None:
            raise ResponseException('Job ID {} not found in database'.format(job_id), StatusCode.CLIENT_ERROR, None,
                                    ValidationError.jobError)

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
