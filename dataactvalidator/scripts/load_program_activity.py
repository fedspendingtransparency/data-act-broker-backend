import os
import logging
import io
import pandas as pd
import numpy as np
import boto3
import datetime
import sys

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import ProgramActivity, ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe
from dataactcore.utils.failure_threshold_exception import FailureThresholdExceededException

logger = logging.getLogger(__name__)

PA_BUCKET = 'da-data-sources'
PA_SUB_KEY = 'OMB_Data/'
PA_FILE_NAME = "DATA Act Program Activity List for Treas.csv"
VALID_HEADERS = {'AGENCY_CODE', 'ALLOCATION_ID', 'ACCOUNT_CODE', 'PA_CODE', 'PA_TITLE', 'FYQ'}


def get_program_activity_file(base_path):
    """ Retrieves the program activity file to load

        Args:
            base_path: directory of domain config files

        Returns:
            the file path for the pa file either on S3 or locally
    """
    if CONFIG_BROKER["use_aws"]:
        s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        s3_object = s3.Object(PA_BUCKET, PA_SUB_KEY + PA_FILE_NAME)
        response = s3_object.get(PA_SUB_KEY+PA_FILE_NAME)
        pa_file = io.BytesIO(response['Body'].read())
    else:
        pa_file = os.path.join(base_path, PA_FILE_NAME)

    return pa_file


def get_date_of_current_pa_upload(base_path):
    """ Gets the last time the file was uploaded to S3, or alternatively the last time the local
        file was modified.

        Args:
            base_path: directory of domain config files

        Returns:
            DateTime object
    """
    if CONFIG_BROKER["use_aws"]:
        last_uploaded = boto3.client('s3', region_name=CONFIG_BROKER['aws_region']). \
            head_object(Bucket=PA_BUCKET, Key=PA_SUB_KEY+PA_FILE_NAME)['LastModified']
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
        last_stored = last_stored_obj.last_load_date
    return last_stored


def set_stored_pa_last_upload(load_datetime):
    """ Set upload date of most recent file we have recorded (Datetime object)

        Args:
            Datetime object representing the timestamp associated with the current file
    """
    sess = GlobalDB.db().session
    last_stored_obj = sess.query(ExternalDataLoadDate).filter_by(
        external_data_type_id=EXTERNAL_DATA_TYPE_DICT['program_activity_upload']).one_or_none()
    if not last_stored_obj:
        last_stored_obj = ExternalDataLoadDate(external_data_type_id=EXTERNAL_DATA_TYPE_DICT['program_activity_upload'],
                                               last_load_date=load_datetime)
        sess.add(last_stored_obj)
    else:
        last_stored_obj.last_load_date = load_datetime
    sess.commit()


def load_program_activity_data(base_path):
    """ Load program activity lookup table.

        Args:
            base_path: directory of domain config files
    """
    last_upload = get_date_of_current_pa_upload(base_path)
    if not (last_upload > get_stored_pa_last_upload()):
        return

    program_activity_file = get_program_activity_file(base_path)

    logger.info('Loading program activity: ' + PA_FILE_NAME)

    with create_app().app_context():
        sess = GlobalDB.db().session
        try:
            data = pd.read_csv(program_activity_file, dtype=str)
        except pd.io.common.EmptyDataError as e:
            log_blank_file()
            exit_if_nonlocal(4)  # exit code chosen arbitrarily, to indicate distinct failure states
            return
        headers = set([header.upper() for header in list(data)])

        if not VALID_HEADERS.issubset(headers):
            logger.error("Missing required headers. Required headers include: %s" % str(VALID_HEADERS))
            exit_if_nonlocal(4)
            return

        try:
            dropped_count, data = clean_data(
                data,
                ProgramActivity,
                {"fyq": "fiscal_year_quarter", "agency_code": "agency_id", "allocation_id": "allocation_transfer_id",
                 "account_code": "account_number", "pa_code": "program_activity_code",
                 "pa_title": "program_activity_name"},
                {"program_activity_code": {"pad_to_length": 4}, "agency_id": {"pad_to_length": 3},
                 "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
                 "account_number": {"pad_to_length": 4}},
                ["agency_id", "program_activity_code", "account_number", "program_activity_name"],
                True
            )
        except FailureThresholdExceededException as e:
            if e.count == 0:
                log_blank_file()
                exit_if_nonlocal(4)
                return
            else:
                count_str = "Application tried to drop {} rows".format(e.count)
                logger.error("Loading of program activity file failed due to exceeded failure threshold. " + count_str)
                exit_if_nonlocal(5)
                return

        sess.query(ProgramActivity).delete()

        # Lowercase Program Activity Name
        data['program_activity_name'] = data['program_activity_name'].apply(lambda x: lowercase_or_notify(x))

        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load. We also need to log them.
        base_count = data.shape[0]
        data.drop_duplicates(inplace=True)
        logger.info("Dropped {} duplicate rows.".format(base_count - data.shape[0]))

        # insert to db
        table_name = ProgramActivity.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    set_stored_pa_last_upload(last_upload)
    logger.info('{} records inserted to {}'.format(num, table_name))

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
    except Exception as e:
        if x and not np.isnan(x):
            logger.info("Program activity of {} was unable to be lowercased. Entered as-is.".format(x))
            return x
        else:
            logger.info("Null value found for program activity name. Entered default value.")  # should not happen
            return "(not provided)"


def log_blank_file():
    """ Helper function for specific reused log message """
    logger.error("File was blank! Not loaded, routine aborted.")


def exit_if_nonlocal(exit_code):
    if not CONFIG_BROKER['local']:
        sys.exit(exit_code)
