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
from dataactcore.models.domainModels import ProgramActivity, ExternalDataLoadDate, ExternalDataType
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe
from dataactcore.utils.failure_threshold_exception import FailureThresholdExceededException

logger = logging.getLogger(__name__)

PA_BUCKET = 'da-data-sources'
PA_SUB_KEY = 'OMB_Data/'
PA_FILE_NAME = "DATA Act Program Activity List for Treas.csv"


def get_program_activity_file(base_path):
    """ Retrieves the program activity file to load

        Args:
            base_path: directory of domain config files

        Returns:
            the file path for the pa file either on S3 or locally
    """
    if CONFIG_BROKER["use_aws"]:
        s3_object = get_s3_object()
        response = s3_object.get(PA_SUB_KEY+PA_FILE_NAME)
        pa_file = io.BytesIO(response['Body'].read())
    else:
        pa_file = os.path.join(base_path, PA_FILE_NAME)

    return pa_file


def get_s3_object():
    """ Gets the s3 connection

        Returns:
            s3bucket connection object
    """
    if CONFIG_BROKER["use_aws"]:
        s3 = boto3.resource('s3')
        s3_object = s3.Object(PA_BUCKET, PA_SUB_KEY + PA_FILE_NAME)
        return s3_object
    raise Exception("Config specifies not to use AWS, cannot call S3 bucket.")


def get_date_of_current_pa_upload(base_path):
    """ Gets the last time the file was uploaded to S3, or alternatively the last time the local
        file was modified.

        Args:
            base_path: directory of domain config files

        Returns:
            DateTime object
    """
    if CONFIG_BROKER["use_aws"]:
        last_uploaded = boto3.client('s3').head_object(Bucket=PA_BUCKET, Key=PA_SUB_KEY+PA_FILE_NAME)['LastModified']
    else:
        pa_file = get_program_activity_file(base_path)
        last_uploaded = datetime.datetime.fromtimestamp(os.path.getmtime(pa_file))
    return last_uploaded


def get_stored_pa_last_upload():
    """ Gets last recorded timestamp from last time file was processed.

        Returns:
            Upload date of most recent file we have recorded (Datetime object)
    """
    sess = GlobalDB.db().session
    last_stored_obj = sess.query(ExternalDataLoadDate).join(ExternalDataType).filter(
        ExternalDataType.name == "program_activity_upload"
        ).one_or_none()
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
    last_stored_obj = sess.query(ExternalDataLoadDate).join(ExternalDataType).filter(
        ExternalDataType.name == "program_activity_upload"
        ).one_or_none()
    if not last_stored_obj:
        data_type = sess.query(ExternalDataType).filter(
            ExternalDataType.name == "program_activity_upload"
            ).one_or_none()
        last_stored_obj = ExternalDataLoadDate(external_data_type_id=data_type.external_data_type_id,
                                               last_load_date=make_date_tz_aware(load_datetime))
        sess.add(last_stored_obj)
    else:
        last_stored_obj.last_load_date = make_date_tz_aware(load_datetime)
    sess.commit()


def load_program_activity_data(base_path):
    """ Load program activity lookup table.

        Args:
            base_path: directory of domain config files
    """
    last_upload = get_date_of_current_pa_upload(base_path)
    if not make_date_tz_aware(last_upload) > make_date_tz_aware(get_stored_pa_last_upload()):
        return
    else:
        set_stored_pa_last_upload(last_upload)

    program_activity_file = get_program_activity_file(base_path)

    logger.info('Loading program activity: ' + PA_FILE_NAME)

    with create_app().app_context():
        sess = GlobalDB.db().session
        try:
            data = pd.read_csv(program_activity_file, dtype=str)
        except pd.io.common.EmptyDataError as e:
            log_blank_file()
            sys.exit(4)  # exit code chosen arbitrarily, to indicate distinct failure states. Approved by Ops.
        try:
            dropped_count, data = clean_data(
                data,
                ProgramActivity,
                {"fyq": "fiscal_year_quarter", "agency_id": "agency_id", "allocation_id": "allocation_transfer_id",
                 "account": "account_number", "pa_code": "program_activity_code", "pa_name": "program_activity_name"},
                {"program_activity_code": {"pad_to_length": 4}, "agency_id": {"pad_to_length": 3},
                 "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
                 "account_number": {"pad_to_length": 4}},
                ["agency_id", "program_activity_code", "account_number", "program_activity_name"],
                True
            )
        except FailureThresholdExceededException as e:
            if e.count == 0:
                log_blank_file()
                sys.exit(4)
            else:
                count_str = "Application tried to drop {} rows ".format(e.count)
                logger.error("Loading of program activity file failed due to exceeded failure threshold. " + count_str)
                sys.exit(5)

        sess.query(ProgramActivity).delete()

        # Lowercase Program Activity Name
        data['program_activity_name'] = data['program_activity_name'].apply(lambda x: lowercase_or_notify(x))

        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load. We also need to log them.

        deduped = data.drop_duplicates()
        dropped_dupes = data[np.invert(data.index.isin(deduped.index))]
        logger.info("Dropped {} duplicate rows.".format(len(dropped_dupes.index)))
        data = deduped
        # insert to db
        table_name = ProgramActivity.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())

        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))

    if dropped_count > 0:
        sys.exit(3)


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
            logger.info(" Program activity of {} was unable to be lowercased. Entered as-is.".format(x))
            return x
        else:
            logger.info(" Null value found for program activity name. Entered default value.")  # should not happen
            return "(not provided)"


def make_date_tz_aware(d):
    """ File storage locally may have TZ-unaware modification dates, so we need this for local operations.

        Args:
            Datetime object

        Returns:
            Timezone-aware datetime object
    """
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        import pytz
        eastern = pytz.timezone('US/Eastern')
        now_aware = eastern.localize(d)
        return now_aware
    else:
        return d


def log_blank_file():
    """ Helper function for specific reused log message """
    logger.error(" File was blank! Not loaded, routine aborted.")
