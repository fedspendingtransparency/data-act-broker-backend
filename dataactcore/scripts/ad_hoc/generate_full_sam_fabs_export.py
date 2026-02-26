import logging
import os
import datetime
import json

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

"""
This script is used to pull updated financial assistance records (from --date to present) for SAM.
It can also run with --auto to poll the specified S3 bucket (BUCKET_NAME/BUCKET_PREFIX}) for the most
recent file that was uploaded, and use the boto3 response for --date.
"""

BUCKET_NAME = CONFIG_BROKER["sam"]["extract"]["bucket_name"]
BUCKET_PREFIX = CONFIG_BROKER["sam"]["extract"]["bucket_prefix"]


FULL_DUMP_QUERY = """
    SELECT unique_award_key,
        afa_generated_unique
    FROM (SELECT unique_award_key,
                 afa_generated_unique,
                 ROW_NUMBER() OVER (PARTITION BY
                     UPPER(afa_generated_unique)
                     ORDER BY updated_at DESC, published_fabs_id DESC
                     ) AS row_num
          FROM published_fabs) duplicates
    WHERE duplicates.row_num = 1
"""


def main():
    now = datetime.datetime.now()
    sess = GlobalDB.db().session

    metrics_json = {"script_name": "generate_full_sam_fabs_export.py", "start_time": str(now)}
    formatted_today = now.strftime("%Y%m%d")

    local_file = os.path.join(os.getcwd(), f"FABS_for_SAM_UNIQUE_KEY_MAPPING_{formatted_today}.csv")

    logger.info("Starting SQL query of active financial assistance records and writing file")
    write_stream_query(
        sess, FULL_DUMP_QUERY, local_file, local_file, True, generate_headers=True, generate_string=False
    )
    logger.info("Completed SQL query, file written")

    metrics_json["duration"] = str(datetime.datetime.now() - now)

    with open("generate_full_sam_fabs_export_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == "__main__":
    configure_logging()
    with create_app().app_context():
        main()
