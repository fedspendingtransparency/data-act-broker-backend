import logging
import xmltodict
import csv
import boto3
import os

import pandas as pd
import datetime
import json

from dataactbroker.helpers.pandas_helper import check_dataframe_diff
from dataactbroker.helpers.script_helper import list_data

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.models.domainModels import CountryCode
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

CC_NAMESPACES = {
    "http://api.nsgreg.nga.mil/schema/genc/3.0": None,
    "http://api.nsgreg.nga.mil/schema/genc/3.0/genc-cmn": None,
}

TERRITORY_LIST = [
    "ASM",  # American Samoa
    "GUM",  # Guam
    "MNP",  # Mariana Islands
    "PRI",  # Puerto Rico
    "VIR",  # Virgin Islands
    "XBK",  # Baker Island
    "XHO",  # Howland Island
    "XJA",  # Johnston Atoll
    "XJV",  # Jarvis Island
    "XKR",  # Kingman Reef
    "XMW",  # Midway Islands
    "XNV",  # Navassa Island
    "XPL",  # Palmyra Atoll
    "XWK",  # Wake Island
]
FREELY_ASSOCIATED_STATES = [
    "FSM",  # Micronesia
    "MHL",  # Marshall Islands
    "PLW",  # Palau
]


def convert_bool_to_str(row):
    return str(row["territory_free_state"])


def load_country_codes(base_path=None, force_reload=False):
    """Load Country Codes into the database.

    Args:
        base_path: directory of domain config files
        force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    now = datetime.datetime.now()
    metrics_json = {
        "script_name": "load_country_codes.py",
        "start_time": str(now),
        "records_deleted": 0,
        "records_provided": 0,
        "duplicates_dropped": 0,
        "records_inserted": 0,
    }

    with create_app().app_context():
        sess = GlobalDB.db().session

        genc_file_name = "genc.xml"
        if CONFIG_BROKER["use_aws"]:
            s3 = boto3.client("s3", region_name=CONFIG_BROKER["aws_region"])
            s3.download_file(
                Bucket=CONFIG_BROKER["public_files_bucket"],
                Key=f"broker_reference_data/{genc_file_name}",
                Filename=genc_file_name,
            )
            cc_file_path = genc_file_name
        else:
            cc_file_path = os.path.join(base_path, genc_file_name)

        with open(cc_file_path, "r") as cc_file:
            resp_dict = xmltodict.parse(cc_file.read(), process_namespaces=True, namespaces=CC_NAMESPACES)
        country_data = list_data(resp_dict["GENCStandardBaseline"]["GeopoliticalEntityEntry"])
        country_list = []

        for country in country_data:
            country_list.append(
                {
                    "country_name": country["name"],
                    "country_code": country["encoding"]["char3Code"],
                    "country_code_2_char": country["encoding"]["char2Code"],
                    "territory": country["encoding"]["char3Code"] in TERRITORY_LIST,
                    "free_state": country["encoding"]["char3Code"] in FREELY_ASSOCIATED_STATES,
                }
            )

        data = pd.DataFrame(country_list)
        data = clean_data(
            data,
            CountryCode,
            {
                "country_code": "country_code",
                "country_name": "country_name",
                "country_code_2_char": "country_code_2_char",
                "territory": "territory",
                "free_state": "free_state",
            },
            {},
        )
        diff_found = check_dataframe_diff(
            data,
            CountryCode,
            ["country_code_id"],
            ["country_code"],
            lambda_funcs=[
                ("territory", lambda row: str(row["territory"])),
                ("free_state", lambda row: str(row["free_state"])),
            ],
        )

        # insert to db if reload required
        if force_reload or diff_found:
            logger.info("Differences found or reload forced, reloading country_code table.")
            # if there's a difference, clear out the old data before adding the new stuff
            metrics_json["records_deleted"] = sess.query(CountryCode).delete()

            # Restart sequence so it's always starting at 1
            sess.execute("ALTER SEQUENCE country_code_country_code_id_seq RESTART")

            num = insert_dataframe(data, CountryCode.__table__.name, sess.connection())
            metrics_json["records_inserted"] = num
            sess.commit()

            if CONFIG_BROKER["use_aws"]:
                cc_filename = "country_codes.csv"

                data.to_csv(
                    cc_filename,
                    index=False,
                    quoting=csv.QUOTE_ALL,
                    header=True,
                    columns=["country_code", "country_code_2_char", "country_name", "territory", "free_state"],
                )

                logger.info("Uploading {} to {}".format(cc_filename, CONFIG_BROKER["public_files_bucket"]))
                s3 = boto3.client("s3", region_name=CONFIG_BROKER["aws_region"])
                s3.upload_file(
                    "country_codes.csv",
                    CONFIG_BROKER["public_files_bucket"],
                    "broker_reference_data/country_codes.csv",
                )
                os.remove(cc_filename)

            # Updating data load dates if the load successfully added new country codes
            update_external_data_load_date(now, datetime.datetime.now(), "country_code")

            logger.info("{} records inserted to country_code table".format(num))
        else:
            logger.info("No differences found, skipping country_code table reload.")

    metrics_json["duration"] = str(datetime.datetime.now() - now)

    with open("load_country_codes_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")
