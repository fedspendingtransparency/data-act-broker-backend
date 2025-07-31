import os
import logging
import io
import pandas as pd
import boto3
import datetime
import json
import argparse

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date, log_blank_file, exit_if_nonlocal
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import ProgramActivityPARK, ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)

PARK_BUCKET = CONFIG_BROKER["data_sources_bucket"]
PARK_SUB_KEY = "OMB_Data/"
PARK_FILE_NAME = "PARK_PROGRAM_ACTIVITY.csv"


def get_park_file(base_path):
    """Retrieves the PARK file to load

    Args:
        base_path: directory of domain config files

    Returns:
        the file path for the PARK file either on S3 or locally
    """
    if CONFIG_BROKER["use_aws"]:
        s3 = boto3.resource("s3", region_name=CONFIG_BROKER["aws_region"])
        s3_object = s3.Object(PARK_BUCKET, PARK_SUB_KEY + PARK_FILE_NAME)
        response = s3_object.get(Key=(PARK_SUB_KEY + PARK_FILE_NAME))
        pa_file = io.BytesIO(response["Body"].read())
    else:
        pa_file = os.path.join(base_path, PARK_FILE_NAME)

    return pa_file


def get_date_of_current_park_upload(base_path):
    """Gets the last time the file was uploaded to S3, or alternatively the last time the local file was modified.

    Args:
        base_path: directory of domain config files

    Returns:
        DateTime object
    """
    if CONFIG_BROKER["use_aws"]:
        last_uploaded = boto3.client("s3", region_name=CONFIG_BROKER["aws_region"]).head_object(
            Bucket=PARK_BUCKET, Key=PARK_SUB_KEY + PARK_FILE_NAME
        )["LastModified"]
        # LastModified is coming back to us in UTC already; just drop the TZ.
        last_uploaded = last_uploaded.replace(tzinfo=None)
    else:
        park_file = get_park_file(base_path)
        last_uploaded = datetime.datetime.fromtimestamp(os.path.getmtime(park_file), datetime.UTC).replace(tzinfo=None)
    return last_uploaded


def get_stored_park_last_upload():
    """Gets last recorded timestamp from last time file was processed.

    Returns:
        Upload date of most recent file we have recorded (Datetime object)
    """
    sess = GlobalDB.db().session
    last_stored_obj = (
        sess.query(ExternalDataLoadDate)
        .filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT["park_upload"])
        .one_or_none()
    )
    if not last_stored_obj:
        # return epoch ts to make sure we load the data the first time through,
        # and ideally any time the data might have been wiped
        last_stored = datetime.datetime.fromtimestamp(0, datetime.UTC).replace(tzinfo=None)
    else:
        last_stored = last_stored_obj.last_load_date_start
    return last_stored


def export_public_park(raw_data):
    """Exports a public copy of the raw file (modified columns)

    Args:
        raw_data: the raw csv data analyzed from the latest program activity file
    """
    export_name = "park.csv"
    logger.info("Exporting loaded PARK file to {}".format(export_name))
    raw_data.to_csv(export_name, index=0)


def load_park_data(base_path, force_reload=False, export=False):
    """Load PARK lookup table.

    Args:
        base_path: directory of domain config files
        force_reload: whether to force a reload
        export: whether to export a public copy of the file

    Returns:
        exit code for nightly runs to indicate skipped, failed, etc. or None
    """
    now = datetime.datetime.now()
    metrics_json = {"script_name": "load_park.py", "start_time": str(now), "records_deleted": 0, "records_inserted": 0}

    logger.info("Checking PARK upload dates to see if we can skip.")
    last_upload = get_date_of_current_park_upload(base_path)
    skipped = False
    if not (last_upload > get_stored_park_last_upload()) and not force_reload:
        logger.info("Skipping load as it's already been done")
        skipped = True
    else:
        logger.info("Getting the PARK file")
        park_file = get_park_file(base_path)

        logger.info("Loading PARK: {}".format(PARK_FILE_NAME))

        with create_app().app_context():
            sess = GlobalDB.db().session
            try:
                raw_data = pd.read_csv(park_file, dtype=str, na_filter=False)
            except pd.errors.EmptyDataError:
                log_blank_file()
                return 4  # exit code chosen arbitrarily, to indicate distinct failure states

            data = clean_data(
                raw_data,
                ProgramActivityPARK,
                {
                    "fy": "fiscal_year",
                    "pd": "period",
                    "alloc_xfer_agency": "allocation_transfer_id",
                    "aid": "agency_id",
                    "main_acct": "main_account_number",
                    "sub_acct": "sub_account_number",
                    "park": "park_code",
                    "park_name": "park_name",
                },
                {
                    "agency_id": {"pad_to_length": 3},
                    "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
                    "main_account_number": {"pad_to_length": 4},
                    "sub_account_number": {"pad_to_length": 3, "keep_null": True},
                },
            )

            metrics_json["records_deleted"] = sess.query(ProgramActivityPARK).delete()

            # insert to db
            table_name = ProgramActivityPARK.__table__.name
            num = insert_dataframe(data, table_name, sess.connection())
            sess.commit()

            if export:
                export_public_park(raw_data)

        end_time = datetime.datetime.now()
        update_external_data_load_date(now, end_time, "park")
        update_external_data_load_date(last_upload, end_time, "park_upload")
        logger.info("{} records inserted to {}".format(num, table_name))
        metrics_json["records_inserted"] = num

        metrics_json["duration"] = str(end_time - now)

    with open("load_park_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)

    if skipped:
        return 6


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Loads in Program Activity data")
    parser.add_argument(
        "-e", "--export", help="If provided, exports a public version of the file locally", action="store_true"
    )
    parser.add_argument("-f", "--force", help="If provided, forces a reload", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")

    exit_code = load_park_data(config_path, force_reload=args.force, export=args.export)
    if exit_code is not None:
        exit_if_nonlocal(exit_code)
