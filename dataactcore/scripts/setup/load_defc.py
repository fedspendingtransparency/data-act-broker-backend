import boto3
import io
import itertools
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import sys

from datetime import datetime

from dataactbroker.helpers.pandas_helper import check_dataframe_diff
from dataactbroker.helpers.script_helper import get_with_exception_hand

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date, log_blank_file, exit_if_nonlocal
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DEFC

from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)

DEFC_BUCKET = CONFIG_BROKER["data_sources_bucket"]
DEFC_SUB_KEY = "OMB_Data/"
DEFC_FILE_NAME = "DEFC_LIST_FOR_USAS.csv"
VALID_HEADERS = {"DEFC_CODE", "DEFC_TITLE"}

GROUP_MAPPINGS = {"covid_19": ["L", "M", "N", "O", "P", "U", "V"], "infrastructure": ["Z", "1"]}
GROUP_MAPPINGS_FLAT = dict(
    list(
        itertools.chain.from_iterable(
            [[(defc, group_name) for defc in defc_list] for group_name, defc_list in GROUP_MAPPINGS.items()]
        )
    )
)


def get_defc_file(base_path):
    """Retrieves the program activity file to load

    Args:
        base_path: directory of domain config files

    Returns:
        the file path for the pa file either on S3 or locally
    """
    if CONFIG_BROKER["use_aws"]:
        s3 = boto3.resource("s3", region_name=CONFIG_BROKER["aws_region"])
        s3_object = s3.Object(DEFC_BUCKET, DEFC_SUB_KEY + DEFC_FILE_NAME)
        response = s3_object.get(Key=(DEFC_SUB_KEY + DEFC_FILE_NAME))
        defc_file = io.BytesIO(response["Body"].read())
    else:
        defc_file = os.path.join(base_path, DEFC_FILE_NAME)
    return defc_file


def derive_pl_data(public_law):
    """Looks up the public law data from GovInfo and Congress.gov

    Args:
        public_law: a public law string ('<congress>-<law number')

    Returns:
        the short title
        url
        the date approved
    """
    short_title = ""
    url = ""
    date_approved = ""
    congress, law_number = public_law.split("-")
    govinfo_url = f"https://www.govinfo.gov/wssearch/getContentDetail?packageId=PLAW-{congress}publ{law_number}"
    govinfo_data = get_with_exception_hand(govinfo_url)
    if govinfo_data and "title" in govinfo_data:
        short_title = govinfo_data["title"]
        # Cutting out the 'Public Law <congress> - <law> - '
        short_title = short_title[11 + len(congress) + 3 + len(law_number) + 3 :]
        # stripping quotes from short title
        for char in ["'", '"', "`"]:
            short_title = short_title.replace(char, "")

        date_approved = govinfo_data["dcMD"]["origDateIssued"]

        url = govinfo_data["download"]["pdflink"]
        url = f"http:{url}"
    return short_title, url, date_approved


def derive_pls_data(public_law):
    """Generates a series of the public law data derived from the public laws string

    Args:
        public_law: the full string containing the public laws

    Returns:
        a series populated with associated public law data
    """
    public_laws = []
    pl_short_titles = []
    urls = []
    dates_approved = []
    pl_nums = re.findall(r"(\d+-\d+)", public_law)

    # Rebuilding the Public Law string (accounting for multiple public laws)
    pl_types = {
        "Nonemergency": "Non-emergency",
        "Emergency": "Emergency",
        "Disaster": "Disaster",
        "Wildfire Suppression": "Wildfire Suppression",
    }
    pl_type = ""
    for pl_type_raw, pl_type_str in pl_types.items():
        if pl_type_raw.lower() in public_law.lower():
            pl_type = f"{pl_type_str} "
            break
    for pl_num in pl_nums:
        public_laws.append(f"{pl_type}P.L. {pl_num}")
        short_title, url, date_approved = derive_pl_data(pl_num)
        pl_short_titles.append(short_title)
        urls.append(url)
        if date_approved:
            dates_approved.append(date_approved)
    if len(pl_nums) == 0:
        public_laws = pl_short_titles = [public_law]

    return pd.Series(
        {
            "Public Law": public_law,
            "Public Laws": public_laws,
            "Public Law Short Title": pl_short_titles,
            "URLs": urls,
            "Earliest Public Law Enactment Date": min(dates_approved) if dates_approved else None,
        }
    )


def apply_defc_derivations(defc_df):
    """Given a base DEFC dataframe with 'DEFC' and 'Public Law', generate a dataframe with the derived elements

    Args:
        defc_df: the defc dataframe

    Returns:
        the same dataframe with additional derived columns
    """
    logger.info("Deriving Public Law Data")
    defc_df = defc_df.merge(defc_df["Public Law"].apply(derive_pls_data), on="Public Law")
    # Ideally we would just update the data inplace
    # but since we're basing the merge and derivations off the original Public Law,
    # it's easier to just drop the old and rename the new
    defc_df = defc_df.drop(columns=["Public Law"])
    defc_df = defc_df.rename(columns={"Public Laws": "Public Law"})

    defc_df["Group Name"] = defc_df.apply(lambda row: GROUP_MAPPINGS_FLAT.get(row["DEFC"], None), axis=1)
    defc_df["Is Valid"] = True

    return defc_df


def add_defc_outliers(defc_df):
    """Given a DEFC dataframe, generate a dataframe with manually added records

    Args:
        defc_df: the defc dataframe

    Returns:
        the same dataframe with manually added records
    """
    # DEFC 9
    covid_defcs = GROUP_MAPPINGS["covid_19"]
    defc_9_title = (
        f"DEFC of '9' Indicates that the data for this row is not related to a COVID-19 P.L."
        f" (DEFC not one of the following: {covid_defcs}), but that the agency has declined to specify"
        f" which other DEFC (or combination of DEFCs, in the case that the money hasn't been split out"
        f" like it would be with a specific DEFC value) applies."
        f" This code was discontinued on July 13, 2021."
    )
    defc_9_df = pd.DataFrame(
        [
            {
                "DEFC": "9",
                "Public Law": [defc_9_title],
                "Public Law Short Title": [defc_9_title],
                "Is Valid": False,
                "URLs": [],
            }
        ]
    )
    defc_df = pd.concat([defc_df, defc_9_df], ignore_index=True)

    # DEFC QQQ
    defc_qqq_title = "Excluded from tracking (uses non-emergency/non-disaster designated appropriations)"
    defc_qqq_df = pd.DataFrame(
        [
            {
                "DEFC": "QQQ",
                "Public Law": [defc_qqq_title],
                "Public Law Short Title": [defc_qqq_title],
                "Is Valid": True,
                "URLs": [],
            }
        ]
    )
    defc_df = pd.concat([defc_df, defc_qqq_df], ignore_index=True)

    return defc_df


def load_defc(base_path, force_reload=False):
    """Loads the DEFC data.

    Args:
        base_path: the path to the local DEFC file (usually validator config)
        force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    start_time = datetime.now()
    metrics_json = {
        "script_name": "load_defc.py",
        "start_time": str(start_time),
        "records_received": 0,
        "new_defc": [],
        "total_defc_count": 0,
    }

    logger.info("Getting raw DEFC file")
    defc_file = get_defc_file(base_path)

    logger.info("Parsing DEFC data")
    with create_app().app_context():
        try:
            raw_data = pd.read_csv(defc_file, dtype=str, na_filter=False)
        except pd.errors.EmptyDataError:
            log_blank_file()
            exit_if_nonlocal(4)  # exit code chosen arbitrarily, to indicate distinct failure states
            return
        headers = set([header.upper() for header in list(raw_data)])

        if not VALID_HEADERS.issubset(headers):
            logger.error("Missing required headers. Required headers include: %s" % str(VALID_HEADERS))
            exit_if_nonlocal(4)
            return
        metrics_json["records_received"] = len(raw_data)
        # Creating a dataframe of the export csv first and then copying columns to match the database
        raw_data = raw_data.rename(columns={"DEFC_CODE": "DEFC", "DEFC_TITLE": "Public Law"})

        logger.info("Applying derivations")
        raw_data = apply_defc_derivations(raw_data)

        logger.info("Adding DEFC outliers")
        raw_data = add_defc_outliers(raw_data)

        # Clear any lingering np.nan's
        raw_data = raw_data.replace({np.nan: None})

        logger.info("Checking for differences in DEFC data")
        defc_mapping = {
            "defc": "code",
            "public_law": "public_laws",
            "public_law_short_title": "public_law_short_titles",
            "group_name": "group",
            "urls": "urls",
            "is_valid": "is_valid",
            "earliest_public_law_enactment_date": "earliest_pl_action_date",
        }
        data = clean_data(raw_data, DEFC, defc_mapping, {})
        diff_found = check_dataframe_diff(data, DEFC, ["defc_id"], ["code"], date_format="%Y-%m-%d")
        sess = GlobalDB.db().session
        if force_reload or diff_found:

            # The only diff should be whenever a new code is added. Noting it here
            if diff_found:
                incoming_defcs = list(data["code"])
                curr_defcs = [result[0] for result in sess.query(DEFC.code).all()]
                diff_defcs = list(set(incoming_defcs) - set(curr_defcs))
                metrics_json["new_defc"] = diff_defcs
                logger.info(f"Difference found: {diff_defcs}")

            logger.info("Deleting old DEFC data from Broker")
            sess.query(DEFC).delete()

            logger.info("Adding new DEFC data to Broker")
            num = insert_dataframe(data, DEFC.__table__.name, sess.connection())
            sess.commit()
            update_external_data_load_date(start_time, datetime.now(), "defc")
            logger.info("{} records inserted to DEFC".format(num))

            # convert the arrays to pipe-delimited strings
            defc_delim = "|"
            array_cols = ["Public Law", "Public Law Short Title", "URLs"]
            for array_col in array_cols:
                raw_data[array_col] = raw_data[array_col].apply(lambda value: defc_delim.join(value))

            header_order = [
                "DEFC",
                "Public Law",
                "Public Law Short Title",
                "Group Name",
                "URLs",
                "Is Valid",
                "Earliest Public Law Enactment Date",
            ]
            raw_data = raw_data[header_order]
            export_name = "def_codes.csv"
            logger.info("Exporting loaded DEFC file to {}".format(export_name))
            raw_data.to_csv(export_name, index=0)
        else:
            logger.info("No differences found, skipping defc table reload.")

    total_defc_count = sess.query(DEFC).count()
    metrics_json["total_defc_count"] = total_defc_count

    end_time = datetime.now()
    metrics_json["end_time"] = str(end_time)
    metrics_json["duration"] = str(end_time - start_time)

    with open("load_defc_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)

    if not (force_reload or diff_found):
        exit_if_nonlocal(3)
        return


if __name__ == "__main__":
    configure_logging()

    reload = "--force" in sys.argv
    base_path = CONFIG_BROKER["path"]
    validator_config_path = os.path.join(base_path, "dataactvalidator", "config")
    load_defc(validator_config_path, reload)
