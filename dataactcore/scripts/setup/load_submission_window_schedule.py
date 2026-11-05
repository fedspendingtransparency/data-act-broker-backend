import os
import logging
import requests
import boto3
import pandas as pd

from datetime import datetime, timedelta

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.jobModels import SubmissionWindowSchedule

from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def add_day(row, col):
    """Adds 1 day to whatever date is provided (used for adding a day to the deadlines)

    Args:
        row: the row to update
        col: the name of the
    """
    new_date = datetime.strptime(row[col], "%m/%d/%y") + timedelta(days=1)
    return new_date.strftime("%m/%d/%y")


def load_submission_window_schedule():
    """Loads the submission window schedule data."""
    filename = "submission_window_schedule.csv"
    base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    sub_schedule_file = os.path.join(base_path, filename)
    if CONFIG_BROKER["use_aws"]:
        os.remove(sub_schedule_file)

        fapc = os.environ.get("fapc", "false") == "true"
        if fapc:
            s3 = boto3.client("s3")
            bucket = CONFIG_BROKER["public_files_bucket"]
            key = f"broker_reference_data/{filename}"
            s3.download_file(Bucket=bucket, Key=key, Filename=sub_schedule_file)
        else:
            source = f"{CONFIG_BROKER['usas_public_reference_url']}/{filename}"
            r = requests.get(source, allow_redirects=True)
            open(sub_schedule_file, "wb").write(r.content)

    logger.info("Loading submission window schedule data")
    with create_app().app_context():
        data = pd.read_csv(sub_schedule_file, dtype=str)

        data = clean_data(
            data,
            SubmissionWindowSchedule,
            {
                "year": "year",
                "period": "period",
                "period_start": "period_start",
                "publish_deadline": "publish_deadline",
                "certification_deadline": "certification_deadline",
            },
            {},
        )

        # Add a day to the deadlines because the dates in the file are supposed to be inclusive
        data["publish_deadline"] = data.apply(lambda x: add_day(x, "publish_deadline"), axis=1)
        data["certification_deadline"] = data.apply(lambda x: add_day(x, "certification_deadline"), axis=1)

        sess = GlobalDB.db().session
        # delete any data in the SubmissionWindowSchedule table
        sess.query(SubmissionWindowSchedule).delete()

        # insert data into table
        num = insert_dataframe(data, SubmissionWindowSchedule.__table__.name, sess.connection())
        logger.info("{} records inserted to submission_window_schedule".format(num))
        sess.commit()


if __name__ == "__main__":
    configure_logging()
    load_submission_window_schedule()
