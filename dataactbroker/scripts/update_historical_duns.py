import logging
import boto3
import os
import pandas as pd
import argparse
from datetime import datetime

from dataactbroker.helpers.generic_helper import batch
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe
from dataactvalidator.health_check import create_app
from dataactcore.models.domainModels import HistoricDUNS, DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.duns import update_duns_props, LOAD_BATCH_SIZE, update_duns

logger = logging.getLogger(__name__)

HD_COLUMNS = [col.key for col in HistoricDUNS.__table__.columns]


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


def run_duns_batches(file, sess, block_size=LOAD_BATCH_SIZE):
    """ Updates Historic DUNS table in chunks from csv file

        Args:
            file: path to the DUNS export file to use
            sess: the database connection
            block_size: the size of the batches to read from the DUNS export file.
    """
    logger.info("Retrieving total rows from duns file")
    start = datetime.now()

    # CSV column header name in DUNS file
    column_headers = [
        "awardee_or_recipient_uniqu",  # DUNS Field
        "registration_date",  # Registration_Date
        "expiration_date",  # Expiration_Date
        "last_sam_mod_date",  # Last_Update_Date
        "activation_date",  # Activation_Date
        "legal_business_name"  # Legal_Business_Name
    ]
    duns_reader_obj = pd.read_csv(file, skipinitialspace=True, header=None, quotechar='"', dtype=str,
                                  names=column_headers, iterator=True, chunksize=block_size, skiprows=1)
    duns_dfs = [duns_df for duns_df in duns_reader_obj]
    row_count = sum([len(duns_df.index) for duns_df in duns_dfs])
    logger.info("Retrieved row count of {} in {} s".format(row_count, (datetime.now() - start).total_seconds()))

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
            duns_to_load = update_duns_props(duns_to_load)
            column_mappings = {col: col for col in duns_to_load.columns}
            duns_to_load = clean_data(duns_to_load, HistoricDUNS, column_mappings, {})
            duns_added += len(duns_to_load.index)
            update_duns(sess, duns_to_load, HistoricDUNS.__table__.name)
            sess.commit()

            logger.info("Finished updating {} DUNS rows in {} s".format(len(duns_to_load.index),
                                                                        (datetime.now() - start).total_seconds()))

    logger.info("Imported {} historical duns".format(duns_added))


def reload_from_sam(sess):
    """ Reload current historic duns data from SAM to pull in any new columns or data

        Args:
            sess: database connection
    """
    historic_duns_to_update = sess.query(HistoricDUNS.awardee_or_recipient_uniqu).all()
    for duns_batch in batch(historic_duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame(columns=['awardee_or_recipient_uniqu'])
        df = df.append(duns_batch)
        df = update_duns_props(df)
        update_duns(sess, df, table_name=HistoricDUNS.__table__.name)


def clean_historic_duns(sess):
    """ Removes historic DUNS that now appear in SAM csvs

        Args:
            sess: the database connection
    """
    new_duns = list(sess.query(DUNS.awardee_or_recipient_uniqu).filter(
        DUNS.awardee_or_recipient_uniqu == HistoricDUNS.awardee_or_recipient_uniqu, DUNS.historic.is_(False)).all())
    if new_duns:
        logger.info('Found {} new DUNS that were previously only available as a historic DUNS. Removing the historic'
                    ' records from the historic duns table.'.format(len(new_duns)))
        sess.query(HistoricDUNS).filter(HistoricDUNS.awardee_or_recipient_uniqu.in_(new_duns))\
            .delete(synchronize_session=False)
        sess.commit()


def import_historic_duns(sess):
    """ Copy the historic DUNS to the DUNS table

        Args:
            sess: the database connection
    """

    logger.info('Copying historic duns values to DUNS table')
    from_columns = ['hd.{}'.format(column) for column in HD_COLUMNS]
    copy_sql = """
        INSERT INTO duns (
            {columns},
            historic
        )
        SELECT
            {from_columns},
            TRUE
        FROM historic_duns AS hd
        WHERE NOT EXISTS (
            SELECT 1
            FROM duns
            WHERE duns.awardee_or_recipient_uniqu = hd.awardee_or_recipient_uniqu
        );
    """.format(columns=', '.join(HD_COLUMNS), from_columns=', '.join(from_columns))
    sess.execute(copy_sql)
    sess.commit()
    logger.info('Copied historic duns values to DUNS table')


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Adding historical DUNS to Broker.')
    parser.add_argument('--block_size', '-s', help='Number of rows to batch load', type=int, default=LOAD_BATCH_SIZE)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--reload_file', '-r', action='store_true', help='Reload HistoricDUNS table from file and'
                                                                         ' update from SAM')
    action.add_argument('--update_from_sam', '-s', action='store_true', help='Update the current HistoricDUNS with any'
                                                                         'new columns or updated data')
    return parser


def main():
    """ Loads DUNS from the DUNS export file (comprised of DUNS pre-2014) """
    parser = get_parser()
    args = parser.parse_args()
    reload_file = args.reload_file
    update_from_sam = args.update_cols
    block_size = args.block_size

    sess = GlobalDB.db().session

    if reload_file:
        logger.info('Retrieving historical DUNS file')
        start = datetime.now()
        if CONFIG_BROKER["use_aws"]:
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            duns_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['archive_bucket'],
                                                                        'Key': "DUNS_export_deduped.csv"},
                                                         ExpiresIn=10000)
        else:
            duns_file = os.path.join(CONFIG_BROKER["broker_files"], "DUNS_export_deduped.csv")

        if not duns_file:
            raise OSError("No DUNS_export_deduped.csv found.")

        logger.info("Retrieved historical DUNS file in {} s".format((datetime.now() - start).total_seconds()))

        try:
            run_duns_batches(duns_file, sess, block_size)
        except Exception as e:
            logger.exception(e)
            sess.rollback()
    else:
        # if we're using an old historic duns table, clean it up before importing
        clean_historic_duns(sess)

        if update_from_sam:
            reload_from_sam(sess)

    # import the historic duns to the current DUNS table
    import_historic_duns(sess)

    sess.close()
    logger.info("Updating historical DUNS complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
