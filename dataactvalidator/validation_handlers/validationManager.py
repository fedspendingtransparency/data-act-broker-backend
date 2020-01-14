import boto3
import csv
import logging
import os
import traceback
import pandas as pd

from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.exc import SQLAlchemyError

from dataactbroker.handlers.submission_handler import populate_submission_error_info
from dataactbroker.helpers.validation_helper import (
    derive_fabs_awarding_sub_tier, derive_fabs_afa_generated_unique, derive_fabs_unique_award_key, derive_unique_id,
    check_required, check_type, check_length, clean_col, clean_numbers, concat_flex)

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (
    create_file_if_needed, write_file_error, mark_file_complete, run_job_checks, mark_job_status,
    populate_job_error_info, get_action_dates
)

from dataactcore.models.domainModels import matching_cars_subquery, Office, concat_tas_dict, concat_display_tas_dict
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT, FIELD_TYPE_DICT
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

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.daims_name, FileColumn.name, FileColumn.name_short, FileColumn.file_id).all()

        self.long_to_short_dict = {}
        self.short_to_long_dict = {}
        self.daims_to_short_dict = {}
        self.short_to_daims_dict = {}

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
                writer.writerow(['', 'Formatting Error', ValidationError.readErrorMsg, '', '', '', '', str(row_number),
                                 ''])
                error_list.record_row_error(job_id, job.filename, 'Formatting Error', ValidationError.readError,
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
        fields = sess.query(FileColumn).filter(FileColumn.file_id == FILE_TYPE_DICT[file_type])\
            .order_by(FileColumn.daims_name.asc()).all()

        expected_headers = []
        required_fields = []
        number_field_types = [FIELD_TYPE_DICT['INT'], FIELD_TYPE_DICT['DECIMAL'], FIELD_TYPE_DICT['LONG']]
        number_fields = []
        boolean_fields = []
        length_fields = []
        padded_fields = []
        for field in fields:
            expected_headers.append(field.name_short)
            if field.field_types_id in number_field_types:
                number_fields.append(field.name_short)
            elif field.field_types_id == FIELD_TYPE_DICT['BOOLEAN']:
                boolean_fields.append(field.name_short)
            if field.required:
                required_fields.append(field.name_short)
            if field.length:
                length_fields.append(field.name_short)
            if field.padded_flag:
                padded_fields.append(field.name_short)
            sess.expunge(field)

        csv_schema = {row.name_short: row for row in fields}

        try:
            extension = os.path.splitext(file_name)[1]
            if not extension or extension.lower() not in ['.csv', '.txt']:
                raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.fileTypeError)

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

            # Count file rows: throws a File Level Error for non-UTF8 characters
            temp_file = open(reader.get_filename(region_name, bucket_name, file_name), encoding='utf-8')
            file_row_count = 0
            header_length = 0
            long_rows = []
            short_rows = []
            for line in csv.reader(temp_file):
                if line:
                    file_row_count += 1
                    line_length = len(line)
                    # Setting the expected length for the file
                    if header_length == 0:
                        header_length = line_length
                    # All lines that are shorter than they should be
                    elif line_length < header_length:
                        short_rows.append(file_row_count)
                    # All lines that are longer than they should be
                    elif line_length > header_length:
                        long_rows.append(file_row_count)
            try:
                temp_file.close()
            except AttributeError:
                # File does not exist, and so does not need to be closed
                pass

            # initializing warning/error files and dataframes
            total_errors = pd.DataFrame(columns=self.report_headers)
            total_warnings = pd.DataFrame(columns=self.report_headers)

            with open(error_file_path, 'w', newline='') as error_file, \
                    open(warning_file_path, 'w', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                error_csv.writerow(self.report_headers)
                warning_csv.writerow(self.report_headers)

            # Generating formatting errors dataframe
            format_error_list = []
            for format_row in sorted(short_rows + long_rows):
                format_error = {
                    'Unique ID': '',
                    'Field Name': 'Formatting Error',
                    'Error Message': ValidationError.readErrorMsg,
                    'Value Provided': '',
                    'Expected Value': '',
                    'Difference': '',
                    'Flex Field': '',
                    'Row Number': str(format_row),
                    'Rule Label': '',
                    'error_type': ValidationError.readError
                }
                format_error_list.append(format_error)
            format_error_df = pd.DataFrame(format_error_list, columns=list(self.report_headers + ['error_type']))

            format_error_df.to_csv(error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                                   mode='a', header=False)

            # Pull file and return info on whether it's using short or long col headers
            reader.open_file(region_name, bucket_name, file_name, fields, bucket_name,
                             self.get_file_name(error_file_name), self.daims_to_short_dict[job.file_type_id],
                             self.short_to_daims_dict[job.file_type_id], is_local=self.is_local)

            # Getting the dataframe for now
            reader.file.seek(0)

            reader_obj = pd.read_csv(reader.file, dtype=str, delimiter=reader.delimiter, error_bad_lines=False,
                                     keep_default_na=False, chunksize=CHUNK_SIZE, warn_bad_lines=False)
            # total_rows = header + long_rows (and will be added on per chunk)
            total_rows = 1 + len(long_rows)
            max_row_number = 1

            for chunk_df in reader_obj:
                logger.info({
                    'message': 'Loading rows starting from {}'.format(max_row_number + 1),
                    'message_type': 'ValidatorInfo',
                    'submission_id': submission_id,
                    'job_id': job_id,
                    'file_type': file_type,
                    'action': 'data_loading',
                    'status': 'start',
                    'start_time': loading_start
                })

                # TODO: Move all this dataframe stuff to some reasonable location
                # Replace whatever the user included so we're using the database headers
                chunk_df.rename(columns=reader.header_dict, inplace=True)

                empty_file = chunk_df.empty

                if not empty_file:
                    chunk_df = chunk_df.applymap(clean_col)

                    # Adding row number
                    chunk_df = chunk_df.reset_index()
                    chunk_df['row_number'] = chunk_df.index + 1 + max_row_number
                    total_rows += len(chunk_df.index)

                    # Increment row numbers if any were ignored being too long
                    for row in sorted(long_rows):
                        chunk_df.loc[chunk_df['row_number'] >= row, 'row_number'] = chunk_df['row_number'] + 1

                    # Setting max row number for chunking purposes
                    max_row_number = chunk_df['row_number'].max()

                    long_rows = [row for row in long_rows if row > max_row_number]

                    # Drop rows that were too short and pandas filled in with Nones
                    chunk_df = chunk_df[~chunk_df['row_number'].isin(short_rows)]

                    chunk_df = chunk_df.drop(['index'], axis=1)

                    # Drop all rows that have 1 or less filled in values (row_number is always filled in so this is how
                    # we have to drop all rows that are just empty)
                    chunk_df.dropna(thresh=2, inplace=True)
                    empty_file = chunk_df.empty

                    flex_data = None
                    if reader.flex_fields:
                        flex_data = chunk_df.loc[:, list(reader.flex_fields + ['row_number'])]

                    if flex_data is not None and not flex_data.empty:
                        flex_data['concatted'] = flex_data.apply(lambda x: concat_flex(x), axis=1)

                    chunk_df = chunk_df[list(expected_headers + ['row_number'])]

                    if not empty_file:
                        for field in padded_fields:
                            chunk_df[field] = chunk_df.apply(
                                lambda x: FieldCleaner.pad_field(csv_schema[field], x[field]), axis=1)

                # list to keep track of rows that fail validations
                error_rows = []

                is_fabs = (file_type == 'fabs')

                # While not done, pull rows in chunks and put them into staging table if they pass the Validator

                required_list = None
                type_list = None
                office_list = {}
                if not empty_file and is_fabs:
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

                # Only do validations if it's not a D file
                if not empty_file and file_type not in ['award', 'award_procurement']:
                    # Cleaning up numbers so they can be inserted properly
                    for field in number_fields:
                        chunk_df[field] = chunk_df.apply(lambda x: clean_numbers(x[field]), axis=1)

                    if is_fabs:
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

                    chunk_df['unique_id'] = chunk_df.apply(lambda x: derive_unique_id(x, is_fabs), axis=1)

                    # Separate each of the checks to their own dataframes, then concat them together
                    req_errors = check_required(chunk_df, required_fields, required_list, self.report_headers,
                                                self.short_to_long_dict[job.file_type_id], flex_data, is_fabs=is_fabs)
                    type_errors = check_type(chunk_df, number_fields + boolean_fields, type_list, self.report_headers,
                                             csv_schema, self.short_to_long_dict[job.file_type_id], flex_data,
                                             is_fabs=is_fabs)
                    type_error_rows = type_errors['Row Number'].tolist()
                    length_errors = check_length(chunk_df, length_fields, self.report_headers, csv_schema,
                                                 self.short_to_long_dict[job.file_type_id], flex_data, type_error_rows)

                    if is_fabs:
                        error_dfs = [req_errors, type_errors, length_errors]
                        warning_dfs = [pd.DataFrame(columns=list(self.report_headers + ['error_type']))]
                    else:
                        error_dfs = [req_errors, type_errors]
                        warning_dfs = [length_errors]

                    total_errors = pd.concat(error_dfs, ignore_index=True)
                    total_warnings = pd.concat(warning_dfs, ignore_index=True)

                    # Converting these to ints because pandas likes to change them to floats randomly
                    total_errors[['Row Number', 'error_type']] = total_errors[['Row Number', 'error_type']].astype(int)
                    total_warnings[['Row Number', 'error_type']] = total_warnings[['Row Number', 'error_type']].\
                        astype(int)

                    error_rows = [int(x) for x in total_errors['Row Number'].tolist()]

                    for index, row in total_errors.iterrows():
                        error_list.record_row_error(job_id, job.filename, row['Field Name'], row['error_type'],
                                                    row['Row Number'], row['Rule Label'], job.file_type_id, None,
                                                    RULE_SEVERITY_DICT['fatal'])

                    for index, row in total_warnings.iterrows():
                        error_list.record_row_error(job_id, job.filename, row['Field Name'], row['error_type'],
                                                    row['Row Number'], row['Rule Label'], job.file_type_id, None,
                                                    RULE_SEVERITY_DICT['warning'])

                    total_errors.drop(['error_type'], axis=1, inplace=True, errors='ignore')
                    total_warnings.drop(['error_type'], axis=1, inplace=True, errors='ignore')

                    # Remove type error rows from original dataframe
                    chunk_df = chunk_df[~chunk_df['row_number'].isin(type_error_rows)]
                    chunk_df.drop(['unique_id'], axis=1, inplace=True)

                total_errors.to_csv(error_file_path, columns=self.report_headers, index=False, quoting=csv.QUOTE_ALL,
                                    mode='a', header=False)
                total_warnings.to_csv(warning_file_path, columns=self.report_headers, index=False,
                                      quoting=csv.QUOTE_ALL, mode='a', header=False)

                if not empty_file:
                    now = datetime.now()
                    chunk_df['created_at'] = now
                    chunk_df['updated_at'] = now
                    chunk_df['job_id'] = job_id
                    chunk_df['submission_id'] = submission_id
                    insert_dataframe(chunk_df, model.__table__.name, sess.connection())

                if not empty_file and flex_data is not None:
                    flex_data.drop(['concatted'], axis=1, inplace=True)
                    flex_data = flex_data[flex_data['row_number'].isin(chunk_df['row_number'])]

                    flex_rows = pd.melt(flex_data, id_vars=['row_number'], value_vars=reader.flex_fields,
                                        var_name='header', value_name='cell')

                    # Filling in all the shared data for these flex fields
                    now = datetime.now()
                    flex_rows['created_at'] = now
                    flex_rows['updated_at'] = now
                    flex_rows['job_id'] = job_id
                    flex_rows['submission_id'] = submission_id
                    flex_rows['file_type_id'] = job.file_type_id

                    # Adding the entire set of flex fields
                    insert_dataframe(flex_rows, FlexField.__table__.name, sess.connection())
                sess.commit()

                logger.info({
                    'message': 'Loaded rows up to {}'.format(max_row_number),
                    'message_type': 'ValidatorInfo',
                    'submission_id': submission_id,
                    'job_id': job_id,
                    'file_type': file_type,
                    'action': 'data_loading',
                    'status': 'start',
                    'start_time': loading_start
                })

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
                'total_rows': total_rows
            })

            if file_type in ('appropriations', 'program_activity', 'award_financial'):
                update_tas_ids(model, submission_id)

            with open(error_file_path, 'a', newline='') as error_file,\
                    open(warning_file_path, 'a', newline='') as warning_file:
                error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # third phase of validations: run validation rules as specified in the schema guidance. These
                # validations are sql-based.
                sql_error_rows = self.run_sql_validations(job, file_type, self.short_to_long_dict[job.file_type_id],
                                                          error_csv, warning_csv, total_rows, error_list)
                error_rows.extend(sql_error_rows)
            error_file.close()
            warning_file.close()

            # stream file to S3 when not local
            if not self.is_local:
                s3_resource = boto3.resource('s3', region_name=region_name)
                # stream error file
                with open(error_file_path, 'rb') as csv_file:
                    s3_resource.Object(bucket_name, self.get_file_name(error_file_name)).put(Body=csv_file)
                csv_file.close()
                os.remove(error_file_path)

                # stream warning file
                with open(warning_file_path, 'rb') as warning_csv_file:
                    s3_resource.Object(bucket_name, self.get_file_name(warning_file_name)).put(Body=warning_csv_file)
                warning_csv_file.close()
                os.remove(warning_file_path)

            # Calculate total number of rows in file that passed validations
            error_rows_unique = set(error_rows)
            total_rows_excluding_header = total_rows - 1
            valid_rows = total_rows_excluding_header - len(error_rows_unique)

            # Update fabs is_valid rows where applicable
            # Update submission to include action dates where applicable
            if is_fabs:
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
            if file_row_count != total_rows:
                raise ResponseException("", StatusCode.CLIENT_ERROR, None, ValidationError.rowCountError)

            # Update job metadata
            job.number_of_rows = total_rows
            job.number_of_rows_valid = valid_rows
            sess.commit()

            error_list.write_all_row_errors(job_id)
            # Update error info for submission
            populate_job_error_info(job)

            if is_fabs:
                # set number of errors and warnings for detached submission
                populate_submission_error_info(submission_id)

            # Mark validation as finished in job tracker
            mark_job_status(job_id, "finished")
            mark_file_complete(job_id, file_name)

        except Exception:
            logger.error({
                'message': 'An exception occurred during validation',
                'message_type': 'ValidatorInfo',
                'submission_id': job.submission_id,
                'job_id': job.job_id,
                'file_type': job.file_type.name,
                'traceback': traceback.format_exc()
            })
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
            writer: CsvWriter object for error file
            warning_writer: CsvWriter object for warning file
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
                writer.writerow([failure.unique_id, field_name, error_msg, failure.failed_value, failure.expected_value,
                                 failure.difference, failure.flex_fields, str(failure.row), failure.original_label])
            elif failure.severity_id == RULE_SEVERITY_DICT['warning']:
                # write to warnings file
                warning_writer.writerow([failure.unique_id, field_name, error_msg, failure.failed_value,
                                         failure.expected_value, failure.difference, failure.flex_fields,
                                         str(failure.row), failure.original_label])
            # labeled errors
            error_list.record_row_error(job_id, job.filename, field_name, failure.error, row_number,
                                        failure.original_label, failure.file_type_id, failure.target_file_id,
                                        failure.severity_id)
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
        writer.writerow(['Formatting Error', ValidationError.writeErrorMsg, '', '', '', '', model.row_number, ''])
        error_list.record_row_error(job.job_id, job.filename, 'Formatting Error', ValidationError.writeError,
                                    model.row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
        return False
    return True


def write_errors(failures, job, short_colnames, writer, warning_writer, row_number, error_list, flex_cols):
    """ Write errors to error database

        Args:
            failures: List of Failures to be written
            job: Current job
            short_colnames: Dict mapping short names to long names
            writer: CsvWriter object for error file
            warning_writer: CsvWriter object for warning file
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors
            flex_cols: all flex columns for this row

        Returns:
            True if any fatal errors were found, False if only warnings are present
    """
    fatal_error_found = False
    # prepare flex cols for all the errors for this row
    flex_col_cells = []
    for flex_col in flex_cols:
        flex_val = flex_col.cell if flex_col.cell else ''
        flex_col_cells.append(flex_col.header + ': ' + flex_val)

    # join the flex column values so we have a string to use, they will all be the same for the same row
    flex_values = ", ".join(flex_col_cells)

    # For each failure, record it in error report and metadata
    for failure in failures:
        # map short column names back to long names
        if failure.field in short_colnames:
            field_name = short_colnames[failure.field]
        else:
            field_name = failure.field

        severity_id = RULE_SEVERITY_DICT[failure.severity]
        try:
            # If error is an int, it's one of our pre-stored messages
            error_type = int(failure.description)
            error_msg = ValidationError.get_error_message(error_type)
        except ValueError:
            # If not, treat it literally
            error_msg = failure.description

        # Get the fail value if it exists
        fail_value = field_name + ": " + failure.value if failure.value else ''

        if failure.severity == 'fatal':
            fatal_error_found = True
            writer.writerow([failure.unique_id, field_name, error_msg, fail_value, failure.expected, '', flex_values,
                             str(row_number), failure.label])
        elif failure.severity == 'warning':
            # write to warnings file
            warning_writer.writerow([failure.unique_id, field_name, error_msg, fail_value, failure.expected, '',
                                     flex_values, str(row_number), failure.label])
        # Non-labeled errors
        error_list.record_row_error(job.job_id, job.filename, field_name, failure.description, row_number,
                                    failure.label, severity_id=severity_id)
    return fatal_error_found
