import os
import logging
import boto3
import pandas as pd

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.jobModels import QuarterlyRevalidationThreshold

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_quarterly_threshold():
    """ Loads the quarterly revalidation threshold data. """
    if CONFIG_BROKER["use_aws"]:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        threshold_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                         'Key': "quarterly_submission_dates.csv"},
                                                          ExpiresIn=600)
    else:
        threshold_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config",
                                      "quarterly_submission_dates.csv")

    logger.info('Loading quarterly revalidation threshold data')
    with create_app().app_context():
        data = pd.read_csv(threshold_file, dtype=str)

        data = clean_data(
            data,
            QuarterlyRevalidationThreshold,
            {"year": "year", "quarter": "quarter", "window_start": "window_start", "window_end": "window_end"},
            {}
        )

        sess = GlobalDB.db().session
        # delete any data in the QuarterlyRevalidationThreshold table
        sess.query(QuarterlyRevalidationThreshold).delete()

        # insert data into table
        num = insert_dataframe(data, QuarterlyRevalidationThreshold.__table__.name, sess.connection())
        logger.info('{} records inserted to quarterly_revalidation_threshold'.format(num))
        sess.commit()


if __name__ == '__main__':
    configure_logging()
    load_quarterly_threshold()
