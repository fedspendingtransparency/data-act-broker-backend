import logging
import boto3
import os
import pandas as pd
import argparse
from datetime import datetime

from dataactcore.utils.parentDuns import sam_config_is_valid
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe
from dataactcore.models.domainModels import HistoricDUNS, DUNS
from dataactvalidator.health_check import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
import dataactcore.utils.parentDuns

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
    'business_types_codes': [],
    'dba_name': None,
    'ultimate_parent_unique_ide': None,
    'ultimate_parent_legal_enti': None
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
    index = 0
    batch_size = 100
    for duns_list in batch(all_duns, batch_size):
        logger.info("Gathering addtional data for historic DUNS records {}-{}".format(index, index + batch_size))
        duns_props_batch = dataactcore.utils.parentDuns.get_duns_props_from_sam(client, duns_list)
        duns_props_batch.drop(column_headers[1:], axis=1, inplace=True, errors='ignore')
        # Adding in blank rows for DUNS where location data was not found
        added_duns_list = []
        if not duns_props_batch.empty:
            added_duns_list = [str(duns) for duns in duns_props_batch['awardee_or_recipient_uniqu'].tolist()]
        empty_duns_rows = []
        for duns in (set(added_duns_list) ^ set(duns_list)):
            empty_duns_row = props_columns.copy()
            empty_duns_row['awardee_or_recipient_uniqu'] = duns
            empty_duns_rows.append(empty_duns_row)
        duns_props_batch = duns_props_batch.append(pd.DataFrame(empty_duns_rows))
        duns_props_df = duns_props_df.append(duns_props_batch)
        index += batch_size
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
    duns_reader_obj = pd.read_csv(file, skipinitialspace=True, header=None,  encoding='latin1', quotechar='"',
                                  dtype=str, names=column_headers, iterator=True, chunksize=block_size, skiprows=1)
    duns_dfs = [duns_df for duns_df in duns_reader_obj]
    row_count = sum([len(duns_df.index) for duns_df in duns_dfs])
    logger.info("Retrieved row count of {} in {} s".format(row_count, (datetime.now()-start).total_seconds()))

    duns_added = 0
    for duns_df in duns_dfs:
        # Remove rows where awardee_or_recipient_uniqu is null
        duns_df = duns_df[duns_df['awardee_or_recipient_uniqu'].notnull()]
        # Ignore old DUNS we already have
        duns_to_load = remove_existing_duns(duns_df, sess)

        if not duns_to_load.empty:
            logger.info("Adding {} DUNS records from historic data".format(len(duns_to_load.index)))
            start = datetime.now()

            # get address info for incoming duns
            duns_to_load = update_duns_props(duns_to_load, client)
            duns_to_load = clean_data(duns_to_load, HistoricDUNS, column_mappings, {})
            duns_added += len(duns_to_load.index)

            insert_dataframe(duns_to_load, HistoricDUNS.__table__.name, sess.connection())
            sess.commit()

            logger.info("Finished updating {} DUNS rows in {} s".format(len(duns_to_load.index),
                                                                        (datetime.now()-start).total_seconds()))

    logger.info("Imported {} historical duns".format(duns_added))


def clean_historic_duns(sess):
    """ Removes historic DUNS that now appear in SAM csvs

        Args:
            sess: the database connection
    """
    new_duns = list(sess.query(DUNS.awardee_or_recipient_uniqu).filter(
        DUNS.awardee_or_recipient_uniqu == HistoricDUNS.awardee_or_recipient_uniqu, DUNS.historic.is_(False)).all())
    if new_duns:
        logger.info('Found {} new DUNS that were previously only available as a historic DUNS. Removing the historic'
                    'records from both tables.'.format(len(new_duns)))
        sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu.in_(new_duns), DUNS.historic.is_(True))\
            .delete(synchronize_session=False)
        sess.query(HistoricDUNS).filter(HistoricDUNS.awardee_or_recipient_uniqu.in_(new_duns))\
            .delete(synchronize_session=False)
        sess.commit()


def import_historic_duns(sess):
    """ Copy the historic DUNS to the DUNS table

        Args:
            sess: the database connection
    """

    logger.info('Copying historic duns values to DUNS table')
    copy_sql = """
        INSERT INTO duns (
            created_at,
            updated_at,
            awardee_or_recipient_uniqu,
            activation_date,
            expiration_date,
            registration_date,
            last_sam_mod_date,
            legal_business_name,
            dba_name,
            ultimate_parent_unique_ide,
            ultimate_parent_legal_enti,
            address_line_1,
            address_line_2,
            city,
            state,
            zip,
            zip4,
            country_code,
            congressional_district,
            business_types_codes,
            historic
        )
        SELECT
            hd.created_at,
            hd.updated_at,
            hd.awardee_or_recipient_uniqu,
            hd.activation_date,
            hd.expiration_date,
            hd.registration_date,
            hd.last_sam_mod_date,
            hd.legal_business_name,
            hd.dba_name,
            hd.ultimate_parent_unique_ide,
            hd.ultimate_parent_legal_enti,
            hd.address_line_1,
            hd.address_line_2,
            hd.city,
            hd.state,
            hd.zip,
            hd.zip4,
            hd.country_code,
            hd.congressional_district,
            hd.business_types_codes,
            TRUE
        FROM historic_duns hd
        WHERE NOT EXISTS (
            SELECT 1
            FROM duns
            WHERE duns.awardee_or_recipient_uniqu = hd.awardee_or_recipient_uniqu
        );
    """
    sess.execute(copy_sql)
    sess.commit()
    logger.info('Copied historic duns values to DUNS table')


def main():
    """ Loads DUNS from the DUNS export file (comprised of DUNS pre-2014) """
    parser = argparse.ArgumentParser(description='Adding historical DUNS to Broker.')
    parser.add_argument('--block_size', '-s', help='Number of rows to batch load', type=int, default=10000)
    parser.add_argument('--reload_file', '-r', action='store_true', help='Reload HistoricDUNS table from file and'
                                                                         ' update from SAM')
    args = parser.parse_args()
    reload_file = args.reload_file
    block_size = args.block_size

    sess = GlobalDB.db().session

    if reload_file:
        client = sam_config_is_valid()

        logger.info('Retrieving historical DUNS file')
        start = datetime.now()
        if CONFIG_BROKER["use_aws"]:
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            duns_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['archive_bucket'],
                                                                        'Key': "DUNS_export_deduped.csv"}, ExpiresIn=10000)
        else:
            duns_file = os.path.join(CONFIG_BROKER["broker_files"], "DUNS_export_deduped.csv")

        if not duns_file:
            raise OSError("No DUNS_export_deduped.csv found.")

        logger.info("Retrieved historical DUNS file in {} s".format((datetime.now()-start).total_seconds()))

        try:
            run_duns_batches(duns_file, sess, client, block_size)
        except Exception as e:
            logger.exception(e)
            sess.rollback()
    else:
        # if we're using an old historic duns table, clean it up before importing
        clean_historic_duns(sess)

    # import the historic duns to the current DUNS table
    import_historic_duns(sess)

    sess.close()
    logger.info("Updating historical DUNS complete")


if __name__ == '__main__':

    with create_app().app_context():
        configure_logging()

        with create_app().app_context():
            main()
