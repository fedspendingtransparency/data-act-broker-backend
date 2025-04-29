import csv
import logging
import multiprocessing as mp
import os
import re
import time
import traceback
from datetime import datetime, timedelta

import boto3
import pandas as pd
import psutil as ps
from sqlalchemy import and_, case, or_

from dataactbroker.handlers.submission_handler import populate_submission_error_info
from dataactbroker.helpers.validation_helper import (
    derive_fabs_awarding_sub_tier, derive_fabs_afa_generated_unique, derive_fabs_unique_award_key,
    check_required, check_type, check_length, concat_flex, process_formatting_errors,
    parse_fields, simple_file_scan, check_field_format, clean_numbers_vectorized, clean_frame_vectorized,
    derive_unique_id_vectorized, update_val_progress)

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (
    create_file_if_needed, write_file_error, mark_file_complete, run_job_checks, mark_job_status,
    populate_job_error_info, get_action_dates, get_utc_now
)
from dataactcore.interfaces.db import db_connection

from dataactcore.models.domainModels import (
    concat_display_tas_dict,
    concat_tas_dict_vectorized,
    CGAC,
    FREC,
    Office,
    SubTierAgency,
)
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import FileColumn
from dataactcore.models.stagingModels import FABS, FlexField, TotalObligations
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job
from dataactcore.models.validationModels import RuleSql, ValidationLabel

from dataactcore.utils.ResponseError import ResponseError
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import report_file_name
from dataactcore.utils.loader_utils import insert_dataframe
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner, StringCleaner

from dataactvalidator.validation_handlers.errorInterface import record_row_error, write_all_row_errors
from dataactvalidator.validation_handlers.validator import cross_validate_sql, validate_file_by_sql
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)

CHUNK_SIZE = CONFIG_BROKER['validator_batch_size']
MULTIPROCESSING_POOLS = CONFIG_BROKER['multiprocessing_pools'] or None
PARALLEL = CONFIG_BROKER['parallel_loading']
BATCH_SQL_VAL_RESULTS = CONFIG_BROKER['batch_sql_validation_results']


class NoLock():
    """ Simple no-op object that bypasses any with statements that'd be used for locking """
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class ValidationManager:
    """ Outer level class, called by flask route """
    report_headers = ['Unique ID', 'Field Name', 'Rule Message', 'Value Provided', 'Expected Value', 'Difference',
                      'Flex Field', 'Row Number', 'Rule Label']
    cross_file_report_headers = ['Unique ID', 'Source File', 'Source Field Name', 'Target File', 'Target Field Name',
                                 'Rule Message', 'Source Value Provided', 'Target Value Provided', 'Difference',
                                 'Source Flex Field', 'Source Row Number', 'Rule Label']

    def __init__(self, is_local=True, directory=''):
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
        self.has_data = False

        # Schema info
        self.csv_schema = {}
        self.fields = {}
        self.parsed_fields = {}
        self.expected_headers = []
        self.long_to_short_dict = {}
        self.short_to_long_dict = {}
        self.gsdm_to_short_dict = {}
        self.short_to_gsdm_dict = {}

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.gsdm_name, FileColumn.name, FileColumn.name_short, FileColumn.file_id).all()

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

            # Get gsdm_to_short_dict filled in
            if not self.gsdm_to_short_dict.get(col.file_id):
                self.gsdm_to_short_dict[col.file_id] = {}
            clean_gsdm = StringCleaner.clean_string(col.gsdm_name, remove_extras=False)
            self.gsdm_to_short_dict[col.file_id][clean_gsdm] = col.name_short

            # Get short_to_gsdm_dict filled in
            if not self.short_to_gsdm_dict.get(col.file_id):
                self.short_to_gsdm_dict[col.file_id] = {}
            self.short_to_gsdm_dict[col.file_id][col.name_short] = col.gsdm_name

        # accounting for the column duns/uei <-> mismatch
        if self.short_to_long_dict:
            self.short_to_long_dict[FILE_TYPE_DICT['fabs']]['uei'] = 'awardeeorrecipientuei'
        if self.short_to_gsdm_dict:
            self.short_to_gsdm_dict[FILE_TYPE_DICT['fabs']]['uei'] = 'AwardeeOrRecipientUEI'

    def get_file_name(self, path):
        """ Return full path of error report based on provided name

            Args:
                path: the name of the file

            Returns:
                the full path of the file
        """
        if self.is_local:
            return os.path.join(self.directory, path)
        # Forcing forward slash here instead of using os.path to write a valid path for S3
        return ''.join(['errors/', path])

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
        self.file_name = job.filename
        self.is_fabs = (self.file_type.name == 'fabs')

        # initializing processing metadata vars for a new validation
        self.reader = CsvReader()
        self.error_list = {}
        self.error_rows = []
        self.total_rows = 0
        self.total_data_rows = 0
        self.short_rows = []
        self.short_pop_rows = []
        self.short_null_rows = []
        self.long_rows = []
        self.long_pop_rows = []
        self.long_null_rows = []
        self.has_data = False
        self.total_proc_obligations = 0
        self.total_asst_obligations = 0
        self.total_obligations = 0
        self.basic_val_progress = 0
        self.tas_progress = 0
        self.sql_val_progress = 0
        self.final_progress = 0

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
        # Reset progress for this job
        self.job.progress = 0
        sess.commit()

        # If local, make the error report directory
        if self.is_local and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        create_file_if_needed(self.job.job_id, self.file_name)

        # Get file size and write to jobs table
        if CONFIG_BROKER['use_aws']:
            file_size = S3Handler.get_file_size(self.file_name)
        else:
            file_size = os.path.getsize(self.file_name)
        self.job.file_size = file_size
        sess.commit()

        # Get fields for this file
        self.fields = sess.query(FileColumn).filter(FileColumn.file_id == FILE_TYPE_DICT[self.file_type.name])\
            .order_by(FileColumn.gsdm_name.asc()).all()
        self.expected_headers, self.parsed_fields = parse_fields(sess, self.fields)
        self.csv_schema = {row.name_short: row for row in self.fields}

        try:
            # Loading data and initial validations
            self.load_file_data(sess, bucket_name, region_name)

            # When we finish the initial data loading we want to set the progress of the basic validations to 100
            self.basic_val_progress = 100
            update_val_progress(sess, self.job, self.basic_val_progress, self.tas_progress, self.sql_val_progress,
                                self.final_progress)

            if self.file_type.name in ('appropriations', 'program_activity', 'award_financial'):
                update_account_nums(self.model, self.submission_id)

                if self.file_type.name == 'award_financial':
                    update_total_obligations(self.submission_id, total_obligations=self.total_obligations,
                                             total_proc_obligations=self.total_proc_obligations,
                                             total_asst_obligations=self.total_asst_obligations)

            # TAS links are now done, unfortunately there's no way to do this incrementally because it's one
            # SQL query. We update outside of the "if" statement to account for D1/D2 generation. It will not affect
            # FABS as the multiplier is 0
            self.tas_progress = 100
            update_val_progress(sess, self.job, self.basic_val_progress, self.tas_progress, self.sql_val_progress,
                                self.final_progress)

            # SQL Validations
            with open(self.error_file_path, 'a', newline='') as error_file, \
                    open(self.warning_file_path, 'a', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # third phase of validations: run validation rules as specified in the schema guidance. These
                # validations are sql-based.
                self.run_sql_validations(self.short_to_long_dict[self.file_type.file_type_id], error_csv, warning_csv)
                self.sql_val_progress = 100
                update_val_progress(sess, self.job, self.basic_val_progress, self.tas_progress, self.sql_val_progress,
                                    self.final_progress)
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                s3 = boto3.client('s3', region_name=region_name)

                s3.upload_file(self.error_file_path, bucket_name, self.get_file_name(self.error_file_name))
                os.remove(self.error_file_path)

                s3.upload_file(self.warning_file_path, bucket_name, self.get_file_name(self.warning_file_name))
                os.remove(self.warning_file_path)

            # Calculate total number of rows in file that passed validations
            error_rows_unique = set(self.error_rows)
            total_rows_excluding_header = self.total_rows - 1
            valid_rows = total_rows_excluding_header - len(error_rows_unique)

            # Update fabs is_valid rows where applicable
            # Update submission to include action dates where applicable
            if self.is_fabs:
                sess.query(FABS). \
                    filter(FABS.row_number.in_(error_rows_unique), FABS.submission_id == self.submission_id). \
                    update({'is_valid': False}, synchronize_session=False)
                sess.commit()
                min_action_date, max_action_date = get_action_dates(self.submission_id)
                sess.query(Submission).filter(Submission.submission_id == self.submission_id). \
                    update({'reporting_start_date': min_action_date, 'reporting_end_date': max_action_date},
                           synchronize_session=False)

            # Update job metadata
            # Total rows = total rows with data + header + short_pop_rows + long_pop_rows
            self.job.number_of_rows = (self.total_data_rows + 1 + len(self.short_pop_rows) + len(self.long_pop_rows))
            self.job.number_of_rows_valid = valid_rows
            sess.commit()

            write_all_row_errors(self.error_list, self.job.job_id)
            # Update error info for submission
            populate_job_error_info(self.job)

            if self.is_fabs:
                # set number of errors and warnings for detached submission
                populate_submission_error_info(self.submission_id)

            self.final_progress = 100
            update_val_progress(sess, self.job, self.basic_val_progress, self.tas_progress, self.sql_val_progress,
                                self.final_progress)

            # Mark validation as finished in job tracker
            mark_job_status(self.job.job_id, 'finished')
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
            if self.reader:
                self.reader.close()

            sess.commit()

            validation_duration = (datetime.now() - validation_start).total_seconds()
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

            self._kill_spawned_processes()

        return True

    def load_file_data(self, sess, bucket_name, region_name):
        """ Loads in the file data and performs initial validations

            Args:
                sess: the database connection
                bucket_name: the bucket to pull the file
                region_name: the region to pull the file
        """
        loading_start = datetime.now()
        num_proc = 1
        if PARALLEL:
            num_proc = MULTIPROCESSING_POOLS or os.cpu_count()
        logger.info({
            'message': 'Beginning data loading {}'.format(self.log_str),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job.job_id,
            'file_type': self.file_type.name,
            'action': 'data_loading',
            'status': 'start',
            'start_time': loading_start,
            'parallel': PARALLEL,
            'num_proc': num_proc
        })

        # Extension Check
        extension = os.path.splitext(self.file_name)[1]
        if not extension or extension.lower() not in ['.csv', '.txt']:
            raise ResponseError('', StatusCode.CLIENT_ERROR, None, ValidationError.file_type_error)

        # Base file check
        file_row_count, self.short_pop_rows, self.long_pop_rows, self.short_null_rows, self.long_null_rows = \
            simple_file_scan(self.reader, bucket_name, region_name, self.file_name)
        self.short_rows = self.short_null_rows + self.short_pop_rows
        self.long_rows = self.long_null_rows + self.long_pop_rows
        # total_rows = header + long_rows (and will be added on per chunk)
        # Note: we're adding long_rows here because pandas will exclude long_rows when we're loading the data
        self.total_rows = 1 + len(self.long_rows)
        self.total_data_rows = 0

        # Making base error/warning files
        self.error_file_name = report_file_name(self.submission_id, False, self.file_type.name)
        self.error_file_path = ''.join([CONFIG_SERVICES['error_report_path'], self.error_file_name])
        self.warning_file_name = report_file_name(self.submission_id, True, self.file_type.name)
        self.warning_file_path = ''.join([CONFIG_SERVICES['error_report_path'], self.warning_file_name])

        with open(self.error_file_path, 'w', newline='') as error_file, \
                open(self.warning_file_path, 'w', newline='') as warning_file:
            error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            error_csv.writerow(self.report_headers)
            warning_csv.writerow(self.report_headers)

        # Adding formatting errors to error file
        format_error_df = process_formatting_errors(self.short_rows, self.long_rows, self.report_headers)
        for index, row in format_error_df.iterrows():
            record_row_error(self.error_list, self.job.job_id, self.file_name, row['Field Name'], row['error_type'],
                             row['Row Number'], row['Rule Label'], self.file_type.file_type_id, None,
                             RULE_SEVERITY_DICT['fatal'])
            self.error_rows.append(row['Row Number'])
        format_error_df.to_csv(self.error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                               mode='a', header=False)

        # Finally open the file for loading into the database with baseline validations
        self.reader.open_file(region_name, bucket_name, self.file_name, self.fields, bucket_name,
                              self.get_file_name(self.error_file_name),
                              self.gsdm_to_short_dict[self.file_type.file_type_id],
                              self.short_to_gsdm_dict[self.file_type.file_type_id],
                              is_local=self.is_local)
        # Going back to reprocess the header row
        self.reader.file.seek(0)

        # getting a list of starting rows to skip if they are long rows (causes indexing issues with pandas)
        skiprows = []
        # pandas skiprow's is 0-indexed whereas long_rows is 1-indexed
        # we're starting from 1 since we know the header is fine
        if len(self.long_rows) > 0:
            for i in range(1, max(self.long_rows) + 1):
                if i + 1 not in self.long_rows:
                    break
                skiprows.append(i)

        reader_obj = pd.read_csv(self.reader.file, dtype=str, delimiter=self.reader.delimiter, na_filter=False,
                                 chunksize=CHUNK_SIZE, on_bad_lines='skip', skiprows=skiprows)
        # Setting this outside of reader/file type objects which may not be used during processing
        self.flex_fields = self.reader.flex_fields
        self.header_dict = self.reader.header_dict
        self.file_type_name = self.file_type.name
        self.file_type_id = self.file_type.file_type_id
        self.job_id = self.job.job_id

        if PARALLEL:
            self.parallel_data_loading(reader_obj, file_row_count)
        else:
            self.iterative_data_loading(reader_obj, file_row_count)

        # Ensure validated rows match initial row count
        if file_row_count != self.total_rows:
            raise ResponseError('', StatusCode.CLIENT_ERROR, None, ValidationError.row_count_error)

        # Add a warning if the file is blank
        if self.file_type.file_type_id in (FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity'],
                                           FILE_TYPE_DICT['award_financial']) \
                and not self.has_data and len(self.short_pop_rows) == 0 and len(self.long_pop_rows) == 0:
            empty_file = {
                'Unique ID': '',
                'Field Name': 'Blank File',
                'Rule Message': ValidationError.blank_file_error_msg,
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': None,
                'Rule Label': 'DABSBLANK',
                'error_type': ValidationError.blank_file_error
            }
            empty_file_df = pd.DataFrame([empty_file], columns=list(self.report_headers + ['error_type']))
            record_row_error(self.error_list, self.job.job_id, self.file_name, empty_file['Field Name'],
                             empty_file['error_type'], empty_file['Row Number'], empty_file['Rule Label'],
                             self.file_type.file_type_id, None, RULE_SEVERITY_DICT['warning'])
            empty_file_df.to_csv(self.warning_file_path, columns=self.report_headers, index=False,
                                 quoting=csv.QUOTE_ALL, mode='a', header=False)

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
            'total_rows': self.total_rows,
            'parallel': PARALLEL,
            'num_proc': num_proc
        })

        return file_row_count

    def parallel_data_loading(self, reader_obj, file_row_count):
        """ The parallelized version of data loading that processes multiple chunks simultaneously

            Args:
                reader_obj: the iterator reader object to iterate over all the chunks
                file_row_count: the total number of rows in the file
        """
        with mp.Manager() as server_manager:
            # These variables will need to be shared among the processes and used later overall
            shared_data = server_manager.dict(
                total_rows=self.total_rows,
                total_data_rows=self.total_data_rows,
                has_data=self.has_data,
                error_rows=self.error_rows,
                error_list=self.error_list,
                errored=False,
                total_proc_obligations=self.total_proc_obligations,
                total_asst_obligations=self.total_asst_obligations,
                total_obligations=self.total_obligations
            )
            # setting reader to none as multiprocess can't pickle it, it'll get reset
            temp_reader = self.reader
            self.reader = None

            # We need to dispose the engine connection when making the child processes in SQLAlchemy 1.4
            # https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
            conn = db_connection()
            engine = conn.engine

            def initializer():
                """ ensure the parent proc's database connections are not touched in the new connection pool """
                engine.dispose(close=False)

            m_lock = server_manager.Lock()
            pool = mp.Pool(MULTIPROCESSING_POOLS, initializer=initializer())
            results = []
            try:
                for chunk_df in reader_obj:
                    result = pool.apply_async(func=self.parallel_process_data_chunk,
                                              args=(chunk_df, shared_data, file_row_count, m_lock))
                    results.append(result)
                pool.close()

                # Raises any exceptions if such occur
                for result in results:
                    result.get()
            except pd.errors.ParserError as e:
                # if pandas can't read a later portion after starting,
                # make sure the pool is closed/joined first
                raise e

            # Resetting these out here as they are used later in the process
            self.total_proc_obligations = round(shared_data['total_proc_obligations'], 2)
            self.total_asst_obligations = round(shared_data['total_asst_obligations'], 2)
            self.total_obligations = round(shared_data['total_obligations'], 2)
            self.total_rows = shared_data['total_rows']
            self.total_data_rows = shared_data['total_data_rows']
            self.has_data = shared_data['has_data']
            self.error_rows = shared_data['error_rows']
            self.error_list = shared_data['error_list']
            self.reader = temp_reader

    def iterative_data_loading(self, reader_obj, file_row_count):
        """ The normal version of data loading that iterates over each chunk

            Args:
                reader_obj: the iterator reader object to iterate over all the chunks
                file_row_count: the total number of rows in the file
        """
        shared_data = dict(
            total_rows=self.total_rows,
            total_data_rows=self.total_data_rows,
            has_data=self.has_data,
            error_rows=self.error_rows,
            error_list=self.error_list,
            total_proc_obligations=self.total_proc_obligations,
            total_asst_obligations=self.total_asst_obligations,
            total_obligations=self.total_obligations
        )
        for chunk_df in reader_obj:
            self.process_data_chunk(chunk_df, shared_data, file_row_count)

        # Resetting these out here as they are used later in the process
        self.total_proc_obligations = round(shared_data['total_proc_obligations'], 2)
        self.total_asst_obligations = round(shared_data['total_asst_obligations'], 2)
        self.total_obligations = round(shared_data['total_obligations'], 2)
        self.total_rows = shared_data['total_rows']
        self.total_data_rows = shared_data['total_data_rows']
        self.has_data = shared_data['has_data']
        self.error_rows = shared_data['error_rows']
        self.error_list = shared_data['error_list']

    def parallel_process_data_chunk(self, chunk_df, shared_data, file_row_count, m_lock=None):
        """ Wrapper around process_data_chunk for parallelization and error catching

            Args:
                chunk_df: the chunk of the file to process as a dataframe
                shared_data: dictionary of shared data among the chunks
                file_row_count: the total number of rows in the file
                m_lock: manager lock if provided to ensure processes don't override each other
        """
        # If one of the processes has errored, we don't want to run any more chunks
        lockable = m_lock if m_lock else NoLock
        with lockable:
            if shared_data['errored']:
                return

        if m_lock:
            # make a new connection per process
            conn = db_connection()
            sess = conn.session

        try:
            self.process_data_chunk(chunk_df, shared_data, file_row_count, sess=sess, lockable=lockable)
        except Exception as e:
            logger.exception(e)
            with lockable:
                shared_data['errored'] = True
            raise e
        finally:
            sess.commit()
            conn.close()
            logging.shutdown()

    def process_data_chunk(self, chunk_df, shared_data, file_row_count, sess=None, lockable=None):
        """ Loads in a chunk of the file and performs initial validations

            Args:
                chunk_df: the chunk of the file to process as a dataframe
                shared_data: dictionary of shared data among the chunks
                file_row_count: the total number of rows in the file
                sess: database connection
                lockable: manager lock if provided to ensure processes don't override each other
        """
        if not sess:
            sess = GlobalDB.db().session
        if not lockable:
            lockable = NoLock()

        if chunk_df.empty:
            logger.warning({
                'message': 'Empty chunk provided.',
                'message_type': 'ValidatorWarning',
                'submission_id': self.submission_id,
                'job_id': self.job_id,
                'file_type': self.file_type_name,
                'action': 'data_loading',
                'status': 'end'
            })
            return

        # initializing warning/error files and dataframes
        total_errors = pd.DataFrame(columns=self.report_headers)
        total_warnings = pd.DataFrame(columns=self.report_headers)
        flex_data = None
        required_list = {}
        type_list = {}
        office_list = {}

        # Replace whatever the user included so we're using the database headers
        chunk_df.rename(columns=self.header_dict, inplace=True)

        # Do a cleanup of any empty/vacuous rows/cells
        chunk_df = clean_frame_vectorized(chunk_df)

        # Adding row number
        chunk_df['index'] = chunk_df.index
        # index gets reset for each chunk, adding the header, and adding previous rows
        chunk_df['row_number'] = chunk_df.index + 2

        with lockable:
            shared_data['total_rows'] += len(chunk_df.index)

        # Increment row numbers if any were ignored being too long
        # This syncs the row numbers back to their original values
        for row in sorted(self.long_rows):
            chunk_df.loc[chunk_df['row_number'] >= row, 'row_number'] = chunk_df['row_number'] + 1

        logger.info({
            'message': 'Loading rows starting from {}'.format(chunk_df['row_number'].iloc[0]),
            'message_type': 'ValidatorInfo',
            'submission_id': self.submission_id,
            'job_id': self.job_id,
            'file_type': self.file_type_name,
            'action': 'data_loading',
            'status': 'start'
        })

        # Drop rows that were too short and pandas filled in with Nones
        chunk_df = chunk_df[~chunk_df['row_number'].isin(self.short_rows)]

        # Drop the index column
        chunk_df = chunk_df.drop(['index'], axis=1)

        # Drop all rows that have 1 or less filled in values (row_number is always filled in so this is how
        # we have to drop all rows that are just empty)
        chunk_df.dropna(thresh=2, inplace=True)

        # Recheck for empty dataframe after cleanup of vacuous rows, and short-circuit if no data left to process
        if chunk_df.empty:
            logger.warning({
                'message': 'Only empty rows found. No data loaded in this chunk',
                'message_type': 'ValidatorWarning',
                'submission_id': self.submission_id,
                'job_id': self.job_id,
                'file_type': self.file_type_name,
                'action': 'data_loading',
                'status': 'end'
            })
            return

        with lockable:
            shared_data['has_data'] = True
            shared_data['total_data_rows'] += len(chunk_df.index)
        if self.is_fabs:
            # create a list of all required/type labels for FABS
            labels = sess.query(ValidationLabel).all()
            for label in labels:
                if label.label_type == 'requirement':
                    required_list[label.column_name] = label.label
                else:
                    type_list[label.column_name] = label.label

            # Create a list of all offices
            offices = sess.query(Office.office_code, Office.sub_tier_code).all()
            for office in offices:
                office_list[office.office_code] = office.sub_tier_code
            # Clear out office list to save space
            del offices

        # Gathering flex data (must be done before chunk limiting)
        if self.flex_fields:
            flex_data = chunk_df.loc[:, list(self.flex_fields + ['row_number'])]
        if flex_data is not None and not flex_data.empty:
            flex_data['concatted'] = flex_data.apply(lambda x: concat_flex(x), axis=1)

        # Dropping any extraneous fields included + flex data (must be done before file type checking)
        chunk_df = chunk_df[list(self.expected_headers + ['row_number'])]

        # Only do validations if it's not a D file
        if self.file_type_name not in ['award', 'award_procurement']:

            # Padding specific fields
            for field in self.parsed_fields['padded']:
                chunk_df[field] = FieldCleaner.pad_field_vectorized(chunk_df[field], self.csv_schema[field])
            # Cleaning up numbers so they can be inserted properly
            for field in self.parsed_fields['number']:
                clean_numbers_vectorized(chunk_df[field])

            if self.is_fabs:
                chunk_df['is_valid'] = True
                chunk_df['awarding_sub_tier_agency_c'] = chunk_df.apply(
                    lambda x: derive_fabs_awarding_sub_tier(x, office_list), axis=1)
                chunk_df['afa_generated_unique'] = chunk_df.apply(
                    lambda x: derive_fabs_afa_generated_unique(x), axis=1)
                agency_codes = self.retrieve_agency_codes(chunk_df, sess)
                chunk_df = chunk_df.merge(agency_codes, how='left', on='awarding_sub_tier_agency_c')
                chunk_df['unique_award_key'] = derive_fabs_unique_award_key(chunk_df)
            else:
                # Updating DEFC QQQ specifically to be a single Q. Only check B and C because they're the only files
                # with DEFC columns
                if self.file_type_name in ['program_activity', 'award_financial']:
                    chunk_df['disaster_emergency_fund_code'] = chunk_df['disaster_emergency_fund_code']. \
                        apply(lambda x: re.sub(re.escape('QQQ'), 'Q', x, flags=re.IGNORECASE) if x else None)
                chunk_df['tas'] = concat_tas_dict_vectorized(chunk_df)
                # display_tas is done with axis=1 (row-wise, where each dict-like row is passed into the given lambda)
                # because the rendering label is not generated consistently, but will have a different rendered-label
                # depending on which TAS components were present and which were not present (NULL, NaN, None)
                chunk_df['display_tas'] = chunk_df.apply(lambda x: concat_display_tas_dict(x), axis=1)
            chunk_df['unique_id'] = derive_unique_id_vectorized(chunk_df, self.is_fabs)

            # Separate each of the checks to their own dataframes, then concat them together
            req_errors = check_required(chunk_df, self.parsed_fields['required'], required_list,
                                        self.report_headers, self.short_to_long_dict[self.file_type_id],
                                        flex_data, is_fabs=self.is_fabs)
            type_errors = check_type(chunk_df, self.parsed_fields['number'] + self.parsed_fields['boolean'],
                                     type_list, self.report_headers, self.csv_schema,
                                     self.short_to_long_dict[self.file_type_id], flex_data,
                                     is_fabs=self.is_fabs)
            type_error_rows = type_errors['Row Number'].tolist()
            length_errors = check_length(chunk_df, self.parsed_fields['length'], self.report_headers,
                                         self.csv_schema, self.short_to_long_dict[self.file_type_id],
                                         flex_data, type_error_rows)
            field_format_errors = check_field_format(chunk_df, self.parsed_fields['format'], self.report_headers,
                                                     self.short_to_long_dict[self.file_type_id],
                                                     flex_data)
            field_format_error_rows = field_format_errors['Row Number'].tolist()

            total_errors = pd.concat([req_errors, type_errors, length_errors, field_format_errors],
                                     ignore_index=True)

            # Converting these to ints because pandas likes to change them to floats randomly
            total_errors[['Row Number', 'error_type']] = total_errors[['Row Number', 'error_type']].astype(int)

            with lockable:
                shared_data['error_rows'] = (shared_data['error_rows']
                                             + [int(x) for x in total_errors['Row Number'].tolist()])

            with lockable:
                for index, row in total_errors.iterrows():
                    shared_data['error_list'] = record_row_error(shared_data['error_list'], self.job_id, self.file_name,
                                                                 row['Field Name'], row['error_type'],
                                                                 row['Row Number'], row['Rule Label'],
                                                                 self.file_type_id, None, RULE_SEVERITY_DICT['fatal'])

            total_errors.drop(['error_type'], axis=1, inplace=True, errors='ignore')

            # Remove type error rows from original dataframe
            chunk_df = chunk_df[~chunk_df['row_number'].isin(type_error_rows + field_format_error_rows)]
            chunk_df.drop(['unique_id'], axis=1, inplace=True)

        # Write all the errors/warnings to their files
        total_errors.to_csv(self.error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                            mode='a', header=False)
        total_warnings.to_csv(self.warning_file_path, columns=self.report_headers, index=False,
                              quoting=csv.QUOTE_ALL, mode='a', header=False)

        # Finally load the data into the database
        # The model data
        now = datetime.now()
        chunk_df['created_at'] = now
        chunk_df['updated_at'] = now
        chunk_df['job_id'] = self.job_id
        chunk_df['submission_id'] = self.submission_id
        if self.is_fabs:
            chunk_df['uei'] = chunk_df['awardee_or_recipient_uei']
            chunk_df.drop(columns=['awardee_or_recipient_uei'], axis=1, inplace=True)

        insert_dataframe(chunk_df, self.model.__table__.name, sess.connection(), method='copy')

        # Update running totals
        if self.file_type_name == 'award_financial':
            chunk_df['transaction_obligated_amou'] = chunk_df['transaction_obligated_amou'].astype(float).fillna(0)
            with lockable:
                shared_data['total_proc_obligations'] += chunk_df.loc[
                    chunk_df['piid'].notna(), 'transaction_obligated_amou'
                ].sum()
                shared_data['total_asst_obligations'] += chunk_df.loc[
                    (chunk_df['fain'].notna()) | (chunk_df['uri'].notna()), 'transaction_obligated_amou'
                ].sum()
                shared_data['total_obligations'] += chunk_df['transaction_obligated_amou'].sum()

        # Flex Fields
        if flex_data is not None:
            flex_data.drop(['concatted'], axis=1, inplace=True)
            flex_data = flex_data[flex_data['row_number'].isin(chunk_df['row_number'])]

            flex_rows = pd.melt(flex_data, id_vars=['row_number'], value_vars=self.flex_fields, var_name='header',
                                value_name='cell')

            # Filling in all the shared data for these flex fields
            now = datetime.now()
            flex_rows['created_at'] = now
            flex_rows['updated_at'] = now
            flex_rows['job_id'] = self.job_id
            flex_rows['submission_id'] = self.submission_id
            flex_rows['file_type_id'] = self.file_type_id

            # Adding the entire set of flex fields
            rows_inserted = insert_dataframe(flex_rows, FlexField.__table__.name, sess.connection(), method='copy')
            logger.info({
                'message': 'Loaded {} flex field rows for batch'.format(rows_inserted),
                'message_type': 'ValidatorInfo',
                'submission_id': self.submission_id,
                'job_id': self.job_id,
                'file_type': self.file_type_name,
                'action': 'data_loading',
                'status': 'end'
            })

        with lockable:
            sess.commit()
            # Seeing how far into the file we currently are
            self.basic_val_progress = shared_data['total_data_rows'] / file_row_count * 100
            update_val_progress(sess, self.job, self.basic_val_progress, self.tas_progress, self.sql_val_progress,
                                self.final_progress)
        if not chunk_df.empty:
            logger.info({
                'message': 'Loaded rows up to {}'.format(chunk_df['row_number'].iloc[-1]),
                'message_type': 'ValidatorInfo',
                'submission_id': self.submission_id,
                'job_id': self.job_id,
                'file_type': self.file_type_name,
                'action': 'data_loading',
                'status': 'end'
            })

    @staticmethod
    def retrieve_agency_codes(chunk_df, sess):
        return pd.read_sql(
            sess.query(
                SubTierAgency.sub_tier_agency_code.label('awarding_sub_tier_agency_c'),
                case((SubTierAgency.is_frec, FREC.frec_code), else_=CGAC.cgac_code).label('awarding_agency_code')
            )
            .join(CGAC, SubTierAgency.cgac_id==CGAC.cgac_id)
            .join(FREC, SubTierAgency.frec_id==FREC.frec_id)
            .filter(SubTierAgency.sub_tier_agency_code.in_(chunk_df['awarding_sub_tier_agency_c']))
            .statement,
            sess.connection()
        )

    def run_sql_validations(self, short_colnames, writer, warning_writer):
        """ Run all SQL rules for this file type

        Args:
            short_colnames: Dict mapping short field names to long
            writer: CsvWriter object for error file
            warning_writer: CsvWriter object for warning file

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        for failure in validate_file_by_sql(self.job, self.file_type.name,
                                            self.short_to_long_dict[self.file_type.file_type_id],
                                            batch_results=BATCH_SQL_VAL_RESULTS):
            # convert shorter, machine friendly column names used in the
            # SQL validation queries back to their long names
            if failure.field_name in short_colnames:
                field_name = short_colnames[failure.field_name]
            else:
                field_name = failure.field_name

            if failure.severity_id == RULE_SEVERITY_DICT['fatal']:
                self.error_rows.append(failure.row)

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
            record_row_error(self.error_list, self.job.job_id, self.file_name, field_name, failure.error,
                             self.total_rows, failure.original_label, failure.file_type_id, failure.target_file_id,
                             failure.severity_id)

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
        error_list = {}

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
        pairs_finished = 0

        # for each cross-file combo, run associated rules and create error report
        cross_list = {
            'program_activity': 'appropriations',
            'award_financial': 'program_activity',
            'award_procurement': 'award_financial',
            'award': 'award_financial'
        }
        for first_file, second_file in cross_list.items():
            first_file_id = FILE_TYPE_DICT[first_file]
            second_file_id = FILE_TYPE_DICT[second_file]
            combo_rules = cross_file_rules.filter(
                or_(
                    and_(RuleSql.file_id == first_file_id, RuleSql.target_file_id == second_file_id),
                    and_(RuleSql.file_id == second_file_id, RuleSql.target_file_id == first_file_id)
                ))

            # get error file name/path
            error_file_name = report_file_name(submission_id, False, second_file, first_file)
            error_file_path = ''.join([CONFIG_SERVICES['error_report_path'], error_file_name])
            warning_file_name = report_file_name(submission_id, True, second_file, first_file)
            warning_file_path = ''.join([CONFIG_SERVICES['error_report_path'], warning_file_name])

            # open error report and gather failed rules within it
            with open(error_file_path, 'w', newline='') as error_file, \
                    open(warning_file_path, 'w', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # write headers to file
                error_csv.writerow(self.cross_file_report_headers)
                warning_csv.writerow(self.cross_file_report_headers)

                # send comboRules to validator.crossValidate sql
                current_cols_short_to_long = self.short_to_long_dict[first_file_id].copy()
                current_cols_short_to_long.update(self.short_to_long_dict[second_file_id].copy())
                cross_validate_sql(combo_rules.all(), submission_id, current_cols_short_to_long, job_id, error_csv,
                                   warning_csv, error_list, pairs_finished, job, batch_results=BATCH_SQL_VAL_RESULTS)
            # close files
            error_file.close()
            warning_file.close()

            # upload file to S3 when not local
            if not self.is_local:
                region_name = CONFIG_BROKER['aws_region']
                bucket_name = CONFIG_BROKER['aws_bucket']
                s3 = boto3.client('s3', region_name=region_name)

                s3.upload_file(error_file_path, bucket_name, self.get_file_name(error_file_name))
                os.remove(error_file_path)

                s3.upload_file(warning_file_path, bucket_name, self.get_file_name(warning_file_name))
                os.remove(warning_file_path)

            pairs_finished += 1

        # write all recorded errors to database
        write_all_row_errors(error_list, job_id)
        # Update error info for submission
        populate_job_error_info(job)

        # mark job status as 'finished'
        mark_job_status(job_id, 'finished')
        job_duration = (datetime.now() - job_start).total_seconds()
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
        job.progress = 100
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
            raise ResponseError('Job ID {} not found in database'.format(job_id), StatusCode.CLIENT_ERROR, None,
                                ValidationError.job_error)

        # Make sure job's prerequisites are complete
        if not run_job_checks(job_id):
            validation_error_type = ValidationError.job_error
            write_file_error(job_id, None, validation_error_type)
            raise ResponseError('Prerequisites for Job ID {} are not complete'.format(job_id),
                                StatusCode.CLIENT_ERROR, None, validation_error_type)

        # Make sure this is a validation job
        if job.job_type.name in ('csv_record_validation', 'validation'):
            job_type_name = job.job_type.name
        else:
            validation_error_type = ValidationError.job_error
            write_file_error(job_id, None, validation_error_type)
            raise ResponseError(
                'Job ID {} is not a validation job (job type is {})'.format(job_id, job.job_type.name),
                StatusCode.CLIENT_ERROR, None, validation_error_type)

        # set job status to running and do validations
        mark_job_status(job_id, 'running')
        if job_type_name == 'csv_record_validation':
            self.run_validation(job)
        elif job_type_name == 'validation':
            self.run_cross_validation(job)
        else:
            raise ResponseError('Bad job type for validator', StatusCode.INTERNAL_ERROR)

        # Update last validated date
        job.last_validated = get_utc_now()
        sess.commit()
        return JsonResponse.create(StatusCode.OK, {'message': 'Validation complete'})

    def _kill_spawned_processes(self):
        """Cleanup (kill) any spawned child processes during this job run"""
        job = ps.Process(os.getpid())
        for spawn_of_job in job.children(recursive=True):
            logger.error({
                'message': 'Attempting to terminate child process with PID: {} and name {}'.format(spawn_of_job.pid,
                                                                                                   spawn_of_job.name),
                'message_type': 'ValidatorInfo',
                'submission_id': self.submission_id,
                'job_id': self.job_id,
            })
            if spawn_of_job.is_running():
                spawn_of_job.kill()


def update_account_nums(model_class, submission_id):
    sess = GlobalDB.db().session

    submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
    start_date = submission.reporting_start_date
    end_date = submission.reporting_end_date
    day_after_end = end_date + timedelta(days=1)

    logger.info({
        'message': 'Setting up TAS links',
        'message_type': 'ValidatorInfo',
        'submission_id': submission_id,
        'model_class': str(model_class)
    })
    start = time.time()

    update_query = """
        WITH relevant_tas AS  (
            SELECT
                min(tas_lookup.account_num) AS min_account_num,
                tas
            FROM
                tas_lookup
            WHERE
                (('{start}'::date, '{end}'::date) OVERLAPS
                    (tas_lookup.internal_start_date, coalesce(tas_lookup.internal_end_date, '{day_after_end}'::date)))
            GROUP BY
                tas
        )
        UPDATE {model}
        SET account_num = min_account_num
        FROM relevant_tas
        WHERE {model}.submission_id = {submission_id}
            AND relevant_tas.tas = {model}.tas;
    """
    full_query = update_query.format(start=start_date, end=end_date, day_after_end=day_after_end,
                                     submission_id=submission_id, model=model_class.__table__.name)
    sess.execute(full_query)

    logger.info({
        'message': 'Completed setting up TAS links',
        'message_type': 'ValidatorInfo',
        'submission_id': submission_id,
        'model_class': str(model_class),
        'duration': time.time() - start
    })

    sess.commit()


def update_total_obligations(submission_id, total_obligations, total_proc_obligations, total_asst_obligations):
    """ Simply updates the total obligations record for a submission

        Args:
            submission_id: submission to update
            total_obligations: total obligations for file C
            total_proc_obligations: total procurement obligations for file C
            total_asst_obligations: total financial assistance obligations for file C
    """
    sess = GlobalDB.db().session

    # Delete existing total obligations if such exist
    sess.query(TotalObligations).filter(TotalObligations.submission_id == submission_id).delete()
    sess.commit()

    new_totals = TotalObligations(submission_id=submission_id, total_obligations=total_obligations,
                                  total_proc_obligations=total_proc_obligations,
                                  total_asst_obligations=total_asst_obligations)
    sess.add(new_totals)
    sess.commit()
