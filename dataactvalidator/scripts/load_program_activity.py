import os
import logging
import io
import pandas as pd
import numpy as np
import boto3
import datetime
import sys
import json
import re
import argparse

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import ProgramActivity, ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe
from dataactcore.utils.failure_threshold_exception import FailureThresholdExceededException

logger = logging.getLogger(__name__)

PA_BUCKET = CONFIG_BROKER['data_sources_bucket']
PA_SUB_KEY = 'OMB_Data/'
PA_FILE_NAME = 'DATA Act Program Activity List for Treas.csv'
VALID_HEADERS = {'AGENCY_CODE', 'ALLOCATION_ID', 'ACCOUNT_CODE', 'PA_CODE', 'PA_TITLE', 'FYQ'}


def get_program_activity_file(base_path):
    """ Retrieves the program activity file to load

        Args:
            base_path: directory of domain config files

        Returns:
            the file path for the pa file either on S3 or locally
    """
    if CONFIG_BROKER['use_aws']:
        s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        s3_object = s3.Object(PA_BUCKET, PA_SUB_KEY + PA_FILE_NAME)
        response = s3_object.get(Key=(PA_SUB_KEY + PA_FILE_NAME))
        pa_file = io.BytesIO(response['Body'].read())
    else:
        pa_file = os.path.join(base_path, PA_FILE_NAME)

    return pa_file


def get_date_of_current_pa_upload(base_path):
    """ Gets the last time the file was uploaded to S3, or alternatively the last time the local file was modified.

        Args:
            base_path: directory of domain config files

        Returns:
            DateTime object
    """
    if CONFIG_BROKER['use_aws']:
        last_uploaded = boto3.client('s3', region_name=CONFIG_BROKER['aws_region']). \
            head_object(Bucket=PA_BUCKET, Key=PA_SUB_KEY + PA_FILE_NAME)['LastModified']
        # LastModified is coming back to us in UTC already; just drop the TZ.
        last_uploaded = last_uploaded.replace(tzinfo=None)
    else:
        pa_file = get_program_activity_file(base_path)
        last_uploaded = datetime.datetime.utcfromtimestamp(os.path.getmtime(pa_file))
    return last_uploaded


def get_stored_pa_last_upload():
    """ Gets last recorded timestamp from last time file was processed.

        Returns:
            Upload date of most recent file we have recorded (Datetime object)
    """
    sess = GlobalDB.db().session
    last_stored_obj = sess.query(ExternalDataLoadDate).filter_by(
        external_data_type_id=EXTERNAL_DATA_TYPE_DICT['program_activity_upload']).one_or_none()
    if not last_stored_obj:
        # return epoch ts to make sure we load the data the first time through,
        # and ideally any time the data might have been wiped
        last_stored = datetime.datetime.utcfromtimestamp(0)
    else:
        last_stored = last_stored_obj.last_load_date_start
    return last_stored


def export_public_pa(raw_data):
    """ Exports a public copy of the raw file (modified columns)

        Args:
            raw_data: the raw csv data analyzed from the latest program activity file
    """
    updated_cols = {
        'fyq': 'REPORTING_PERIOD',
        'agency': 'AGENCY_IDENTIFIER_NAME',
        'allocation_id': 'ALLOCATION_TRANSFER_AGENCY_IDENTIFIER_CODE',
        'agency_code': 'AGENCY_IDENTIFIER_CODE',
        'account_code': 'MAIN_ACCOUNT_CODE',
        'pa_title': 'PROGRAM_ACTIVITY_NAME',
        'pa_code': 'PROGRAM_ACTIVITY_CODE',
        'omb_bureau_title_optnl': 'OMB_BUREAU_TITLE_OPTNL',
        'omb_account_title_optnl': 'OMB_ACCOUNT_TITLE_OPTNL'
    }
    raw_data = raw_data[list(updated_cols.keys())]
    raw_data.columns = [list(updated_cols.values())]

    export_name = 'program_activity.csv'
    logger.info('Exporting loaded PA file to {}'.format(export_name))
    raw_data.to_csv(export_name, index=0)


def load_program_activity_data(base_path, force_reload=False, export=False):
    """ Load program activity lookup table.

        Args:
            base_path: directory of domain config files
            force_reload: whether or not to force a reload
            export: whether or not to export a public copy of the file
    """
    now = datetime.datetime.now()
    metrics_json = {
        'script_name': 'load_program_activity.py',
        'start_time': str(now),
        'records_received': 0,
        'duplicates_dropped': 0,
        'invalid_records_dropped': 0,
        'records_deleted': 0,
        'records_inserted': 0
    }
    dropped_count = 0

    logger.info('Checking PA upload dates to see if we can skip.')
    last_upload = get_date_of_current_pa_upload(base_path)
    if not (last_upload > get_stored_pa_last_upload()) and not force_reload:
        logger.info('Skipping load as it\'s already been done')
    else:
        logger.info('Getting the progrma activity file')
        program_activity_file = get_program_activity_file(base_path)

        logger.info('Loading program activity: {}'.format(PA_FILE_NAME))

        with create_app().app_context():
            sess = GlobalDB.db().session
            try:
                raw_data = pd.read_csv(program_activity_file, dtype=str, na_filter=False)
            except pd.io.common.EmptyDataError:
                log_blank_file()
                exit_if_nonlocal(4)  # exit code chosen arbitrarily, to indicate distinct failure states
                return
            headers = set([header.upper() for header in list(raw_data)])

            if not VALID_HEADERS.issubset(headers):
                logger.error('Missing required headers. Required headers include: %s' % str(VALID_HEADERS))
                exit_if_nonlocal(4)
                return

            try:
                dropped_count, data = clean_data(
                    raw_data,
                    ProgramActivity,
                    {'fyq': 'fiscal_year_period', 'agency_code': 'agency_id', 'allocation_id': 'allocation_transfer_id',
                     'account_code': 'account_number', 'pa_code': 'program_activity_code',
                     'pa_title': 'program_activity_name'},
                    {'program_activity_code': {'pad_to_length': 4}, 'agency_id': {'pad_to_length': 3},
                     'allocation_transfer_id': {'pad_to_length': 3, 'keep_null': True},
                     'account_number': {'pad_to_length': 4}},
                    ['agency_id', 'program_activity_code', 'account_number', 'program_activity_name'],
                    True
                )
            except FailureThresholdExceededException as e:
                if e.count == 0:
                    log_blank_file()
                    exit_if_nonlocal(4)
                    return
                else:
                    logger.error('Loading of program activity file failed due to exceeded failure threshold. '
                                 'Application tried to drop {} rows'.format(e.count))
                    exit_if_nonlocal(5)
                    return

            metrics_json['records_deleted'] = sess.query(ProgramActivity).delete()
            metrics_json['invalid_records_dropped'] = dropped_count

            # Lowercase Program Activity Name
            data['program_activity_name'] = data['program_activity_name'].apply(lambda x: lowercase_or_notify(x))
            # Convert FYQ to FYP
            data['fiscal_year_period'] = data['fiscal_year_period'].apply(lambda x: convert_fyq_to_fyp(x))

            # because we're only loading a subset of program activity info, there will be duplicate records in the
            # dataframe. this is ok, but need to de-duped before the db load. We also need to log them.
            base_count = len(data.index)
            metrics_json['records_received'] = base_count
            data.drop_duplicates(inplace=True)

            dupe_count = base_count - len(data.index)
            logger.info('Dropped {} duplicate rows.'.format(dupe_count))
            metrics_json['duplicates_dropped'] = dupe_count

            # insert to db
            table_name = ProgramActivity.__table__.name
            num = insert_dataframe(data, table_name, sess.connection())
            sess.commit()

            if export:
                export_public_pa(raw_data)

        end_time = datetime.datetime.now()
        update_external_data_load_date(now, end_time, 'program_activity')
        update_external_data_load_date(last_upload, end_time, 'program_activity_upload')
        logger.info('{} records inserted to {}'.format(num, table_name))
        metrics_json['records_inserted'] = num

        metrics_json['duration'] = str(end_time - now)

    with open('load_program_activity_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)

    if dropped_count > 0:
        exit_if_nonlocal(3)
        return


def lowercase_or_notify(x):
    """ Lowercases the input if it is valid, otherwise logs the error and sets a default value

        Args:
            String to lowercase

        Returns:
            Lowercased string if possible, else unmodified string or default value.
    """
    try:
        return x.lower()
    except Exception:
        if x and not np.isnan(x):
            logger.info('Program activity of {} was unable to be lowercased. Entered as-is.'.format(x))
            return x
        else:
            logger.info('Null value found for program activity name. Entered default value.')  # should not happen
            return '(not provided)'


def convert_fyq_to_fyp(fyq):
    """ Converts the fyq provided to fyp if it is in fyq format. Do nothing if it is already in fyp format

        Args:
            fyq: String to convert or leave alone fiscal year quarters

        Returns:
            FYQ converted to FYP or left the same
    """
    # If it's in quarter format, convert to period
    if re.match('^FY\d{2}Q\d$', str(fyq).upper().strip()):
        # Make sure it's all uppercase and replace the Q with a P
        fyq = fyq.upper().strip().replace('Q', 'P')
        # take the last character in the string (the quarter), multiply by 3, replace
        quarter = fyq[-1]
        period = str(int(quarter) * 3).zfill(2)
        fyq = fyq[:-1] + period
        return fyq
    return fyq


def log_blank_file():
    """ Helper function for specific reused log message """
    logger.error('File was blank! Not loaded, routine aborted.')


def exit_if_nonlocal(exit_code):
    if not CONFIG_BROKER['local']:
        sys.exit(exit_code)


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(description='Loads in Program Activit data')
    parser.add_argument('-e', '--export', help='If provided, exports a public version of the file locally',
                        action='store_true')
    parser.add_argument('-f', '--force', help='If provided, forces a reload',
                        action='store_true')
    args = parser.parse_args()

    config_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")

    load_program_activity_data(config_path, force_reload=args.force, export=args.export)
