import logging
import boto
import os
import pandas as pd
import argparse
from datetime import datetime

from dataactcore.models.domainModels import DUNS
from dataactcore.utils.parentDuns import get_location_business_from_sam, sam_config_is_valid
from dataactcore.utils.duns import load_duns_by_row
from dataactvalidator.scripts.loaderUtils import clean_data
from dataactvalidator.health_check import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)

# CSV column header name in DUNS file
column_headers = [
    "awardee_or_recipient_uniqu",  # DUNS Field
    "registration_date",  # Registration_Date
    "expiration_date",  # Expiration_Date
    "last_sam_mod_date",  # Last_Update_Date
    "activation_date",  # Activation_Date
    "legal_business_name"  # Legal_Business_Name
]
props_columns = {
    'address_line_1': None,
    'address_line_2': None,
    'city': None,
    'state': None,
    'zip': None,
    'zip4': None,
    'country_code': None,
    'congressional_district': None,
    'business_types_codes': []
}

column_mappings = {x: x for x in column_headers + list(props_columns.keys())}


def remove_existing_duns(data, sess):
    """ Remove rows from file that already have a entry in broker database. We should only update missing DUNS

        Args:
            data: dataframe representing a list of duns
            sess: the database session

        Returns:
            a new dataframe with the DUNS removed that already exist in the database
    """

    duns_in_file = ",".join(list(data['awardee_or_recipient_uniqu'].unique()))
    sql_query = "SELECT awardee_or_recipient_uniqu " +\
                "FROM duns where awardee_or_recipient_uniqu = ANY('{" + \
                duns_in_file +\
                "}')"

    db_duns = pd.read_sql(sql_query, sess.bind)
    missing_duns = data[~data['awardee_or_recipient_uniqu'].isin(db_duns['awardee_or_recipient_uniqu'])]

    return missing_duns


def clean_duns_csv_data(data):
    """ Simple wrapper around clean_data applied just for duns

        Args:
            data: dataframe representing the data to be cleaned

        Returns:
            a dataframe cleaned and to be imported to the database
    """
    return clean_data(data, DUNS, column_mappings, {})


def batch(iterable, n=1):
    """ Simple function to create batches from a list

        Args:
            iterable: the list to be batched
            n: the size of the batches

        Yields:
            the same list (iterable) in batches depending on the size of N
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def update_duns_props(df, client):
    """ Returns same dataframe with address data updated"

        Args:
            df: the dataframe containing the duns data
            client: the connection to the SAM service

        Returns:
            a merged dataframe with the duns updated with location info from SAM
    """
    all_duns = df['awardee_or_recipient_uniqu'].tolist()
    columns = ['awardee_or_recipient_uniqu'] + list(props_columns.keys())
    duns_props_df = pd.DataFrame(columns=columns)
    # SAM service only takes in batches of 100
    for duns_list in batch(all_duns, 100):
        duns_props_df = duns_props_df.append(get_location_business_from_sam(client, duns_list))
        added_duns = [str(duns) for duns in duns_props_df['awardee_or_recipient_uniqu'].tolist()]
        empty_duns_rows = []
        for duns in duns_list:
            if duns not in added_duns:
                empty_duns_row = props_columns.copy()
                empty_duns_row['awardee_or_recipient_uniqu'] = duns
                empty_duns_rows.append(empty_duns_row)
        duns_props_df = duns_props_df.append(pd.DataFrame(empty_duns_rows))
    return pd.merge(df, duns_props_df, on=['awardee_or_recipient_uniqu'])


def run_duns_batches(file, sess, client, block_size=10000):
    """ Updates DUNS table in chunks from csv file

        Args:
            file: path to the DUNS export file to use
            sess: the database connection
            client: the connection to the SAM service
            block_size: the size of the batches to read from the DUNS export file.
    """
    logger.info("Retrieving total rows from duns file")
    start = datetime.now()
    row_count = len(pd.read_csv(file, skipinitialspace=True, header=None, encoding='latin1', quotechar='"',
                                dtype=str, names=column_headers, skiprows=1))
    logger.info("Retrieved row count of {} in {} s".format(row_count, (datetime.now()-start).total_seconds()))

    duns_reader_obj = pd.read_csv(file, skipinitialspace=True, header=None,  encoding='latin1', quotechar='"',
                                  dtype=str, names=column_headers, iterator=True, chunksize=block_size, skiprows=1)

    for duns_df in duns_reader_obj:
        start = datetime.now()
        # Remove rows where awardee_or_recipient_uniqu is null
        duns_df = duns_df[duns_df['awardee_or_recipient_uniqu'].notnull()]

        duns_to_load = remove_existing_duns(duns_df, sess)
        duns_count = 0

        # Only update database if there are DUNS from file missing in database
        if not duns_to_load.empty:
            duns_count = duns_to_load.shape[0]

            # get address info for incoming duns
            duns_to_load = update_duns_props(duns_to_load, client)

            duns_to_load = clean_duns_csv_data(duns_to_load)
            models = {}
            load_duns_by_row(duns_to_load, sess, models, None)
            sess.commit()

        logger.info("Finished updating {} DUNS rows in {} s".format(duns_count,
                                                                    (datetime.now()-start).total_seconds()))


def main():
    """ Loads DUNS from the DUNS export file (comprised of DUNS pre-2014) """
    parser = argparse.ArgumentParser(description='Adding historical DUNS to Broker.')
    parser.add_argument('-size', '--block_size', help='Number of rows to batch load', type=int,
                        default=10000)
    args = parser.parse_args()

    sess = GlobalDB.db().session
    client = sam_config_is_valid()

    logger.info('Retrieving historical DUNS file')
    start = datetime.now()
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER["archive_bucket"])
        duns_file = s3bucket.get_key("DUNS_export_deduped.csv").generate_url(expires_in=10000)
    else:
        duns_file = os.path.join(CONFIG_BROKER["broker_files"], "DUNS_export_deduped.csv")

    if not duns_file:
        raise OSError("No DUNS_export_deduped.csv found.")

    logger.info("Retrieved historical DUNS file in {} s".format((datetime.now()-start).total_seconds()))

    try:
        run_duns_batches(duns_file, sess, client, args.block_size)
    except Exception as e:
        logger.exception(e)
        sess.rollback()

    logger.info("Updating historical DUNS complete")
    sess.close()


if __name__ == '__main__':

    with create_app().app_context():
        configure_logging()

        with create_app().app_context():
            main()
