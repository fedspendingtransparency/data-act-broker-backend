import logging
import boto
import os
import pandas as pd
import csv
from datetime import datetime

from dataactvalidator.health_check import create_app
from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)

# CSV column header name in DUNS file
column_headers = [
    "DUNS",
    "Registration_Date",
    "Expiration_Date",
    "Last_Update_Date",
    "Activation_Date",
    "Legal_Business_Name"
]


def generate_dedup_export(duns_file, dedup_export_path):
    """Generate dedup duns export"""

    duns_df = pd.read_csv(duns_file, skipinitialspace=True, header=None, encoding='latin1', quotechar='"',
                          dtype=str, names=column_headers, skiprows=1)
    duns_modified_df = duns_df.drop_duplicates(subset=['DUNS'], keep='last')

    return duns_modified_df.to_csv(dedup_export_path, columns=column_headers, index=False, quoting=csv.QUOTE_ALL)


def main():
    logger.info('Retrieving historical DUNS file')
    start = datetime.now()
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER["archive_bucket"])
        duns_file = s3bucket.get_key("DUNS_export.csv").generate_url(expires_in=10000)
    else:
        duns_file = os.path.join(
            CONFIG_BROKER["broker_files"],
            "DUNS_export.csv")

    if not duns_file:
        logger.error("No DUNS_export.csv found.")

    logger.info("Retrieved historical DUNS file in {} s".format((datetime.now()-start).total_seconds()))

    dedup_duns_export = os.path.join(CONFIG_BROKER["broker_files"], 'DUNS_export_dedup.csv')
    generate_dedup_export(duns_file, dedup_duns_export)

    logger.info("{} generated".format(dedup_duns_export))


if __name__ == '__main__':

    with create_app().app_context():
        configure_logging()

        with create_app().app_context():
            main()
