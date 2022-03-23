import logging
import boto3
import os
import pandas as pd
import argparse
from datetime import datetime

from dataactbroker.helpers.generic_helper import batch
from dataactvalidator.scripts.loader_utils import clean_data
from dataactvalidator.health_check import create_app
from dataactcore.models.domainModels import HistoricDUNS, DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.duns import update_sam_props, LOAD_BATCH_SIZE, update_duns

logger = logging.getLogger(__name__)

HD_COLUMNS = [col.key for col in HistoricDUNS.__table__.columns
              if col.key not in ('duns_id', 'created_at', 'updated_at')]


def remove_existing_recipients(data, sess):
    """ Remove rows from file that already have a entry in broker database. We should only update missing recipients

        Args:
            data: dataframe representing a list of recipients
            sess: the database session

        Returns:
            a new dataframe with the recipients removed that already exist in the database
    """

    recps_in_file = ",".join(list(data['awardee_or_recipient_uniqu'].unique()))
    sql_query = "SELECT awardee_or_recipient_uniqu " +\
                "FROM duns where awardee_or_recipient_uniqu = ANY('{" + \
                recps_in_file +\
                "}')"

    db_duns = pd.read_sql(sql_query, sess.bind)
    missing_recps = data[~data['awardee_or_recipient_uniqu'].isin(db_duns['awardee_or_recipient_uniqu'])]

    return missing_recps


def run_sam_batches(file, sess, block_size=LOAD_BATCH_SIZE):
    """ Updates Historic DUNS table in chunks from csv file

        Args:
            file: path to the recipient export file to use
            sess: the database connection
            block_size: the size of the batches to read from the recipient export file.
    """
    logger.info("Retrieving total rows from recipients file")
    start = datetime.now()

    # CSV column header name in recipient file
    column_headers = [
        "awardee_or_recipient_uniqu",  # DUNS Field
        "registration_date",  # Registration_Date
        "expiration_date",  # Expiration_Date
        "last_sam_mod_date",  # Last_Update_Date
        "activation_date",  # Activation_Date
        "legal_business_name"  # Legal_Business_Name
    ]
    sam_reader_obj = pd.read_csv(file, skipinitialspace=True, header=None, quotechar='"', dtype=str,
                                 names=column_headers, iterator=True, chunksize=block_size, skiprows=1)
    sam_dfs = [sam_df for sam_df in sam_reader_obj]
    row_count = sum([len(sam_df.index) for sam_df in sam_dfs])
    logger.info("Retrieved row count of {} in {} s".format(row_count, (datetime.now() - start).total_seconds()))

    recipients_added = 0
    for sam_df in sam_dfs:
        # Remove rows where awardee_or_recipient_uniqu is null
        sam_df = sam_df[sam_df['awardee_or_recipient_uniqu'].notnull()]
        # Ignore old recipients we already have
        recps_to_load = remove_existing_recipients(sam_df, sess)

        if not recps_to_load.empty:
            logger.info("Adding {} SAM records from historic data".format(len(recps_to_load.index)))
            start = datetime.now()

            # get address info for incoming recipients
            recps_to_load = update_sam_props(recps_to_load)
            column_mappings = {col: col for col in recps_to_load.columns}
            recps_to_load = clean_data(recps_to_load, HistoricDUNS, column_mappings, {})
            recipients_added += len(recps_to_load.index)
            update_duns(sess, recps_to_load, HistoricDUNS.__table__.name)
            sess.commit()

            logger.info("Finished updating {} SAM rows in {} s".format(len(recps_to_load.index),
                                                                       (datetime.now() - start).total_seconds()))

    logger.info("Imported {} historical recipients".format(recipients_added))


def reload_from_sam(sess):
    """ Reload current historic recipient data from SAM to pull in any new columns or data

        Args:
            sess: database connection
    """
    historic_recps_to_update = sess.query(HistoricDUNS.awardee_or_recipient_uniqu).all()
    for sam_batch in batch(historic_recps_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame(columns=['awardee_or_recipient_uniqu'])
        df = df.append(sam_batch)
        df = update_sam_props(df)
        update_duns(sess, df, table_name=HistoricDUNS.__table__.name)


def clean_historic_recipients(sess):
    """ Removes historic DUNS that now appear in SAM csvs

        Args:
            sess: the database connection
    """
    new_recps = list(sess.query(DUNS.awardee_or_recipient_uniqu).filter(
        DUNS.awardee_or_recipient_uniqu == HistoricDUNS.awardee_or_recipient_uniqu, DUNS.historic.is_(False)).all())
    if new_recps:
        logger.info('Found {} new DUNS that were previously only available as a historic DUNS. Removing the historic'
                    ' records from the historic duns table.'.format(len(new_recps)))
        sess.query(HistoricDUNS).filter(HistoricDUNS.awardee_or_recipient_uniqu.in_(new_recps))\
            .delete(synchronize_session=False)
        sess.commit()


def import_historic_recipients(sess):
    """ Copy the historic DUNS to the DUNS table

        Args:
            sess: the database connection
    """
    logger.info('Updating historic recipient values in the SAM table')
    update_cols = ['{col} = hd.{col}'.format(col=col) for col in HD_COLUMNS
                   if col not in ['created_at', 'updated_at', 'awardee_or_recipient_uniqu']]
    update_cols.append('updated_at = NOW()')
    # only updating the historic records that are still not updated over time
    update_sql = """
        UPDATE duns
        SET
            {update_cols}
        FROM historic_duns AS hd
        WHERE duns.awardee_or_recipient_uniqu = hd.awardee_or_recipient_uniqu
            AND duns.historic = TRUE;
    """.format(update_cols=','.join(update_cols))
    sess.execute(update_sql)
    logger.info('Updated historic recipient values to SAM table')

    logger.info('Inserting historic recipient values to SAM table')
    from_columns = ['hd.{}'.format(column) for column in HD_COLUMNS]
    copy_sql = """
        INSERT INTO duns (
            {columns},
            historic,
            updated_at,
            created_at
        )
        SELECT
            {from_columns},
            TRUE,
            NOW(),
            NOW()
        FROM historic_duns AS hd
        WHERE NOT EXISTS (
            SELECT 1
            FROM duns
            WHERE duns.awardee_or_recipient_uniqu = hd.awardee_or_recipient_uniqu
        );
    """.format(columns=', '.join(HD_COLUMNS), from_columns=', '.join(from_columns))
    sess.execute(copy_sql)
    sess.commit()
    logger.info('Inserted new historic recipient values to SAM table')


def main():
    """
        Loads recipients from the legacy recipients export file (comprised of recipients pre-2014).
        Note: Should only run after importing all the SAM csv data to prevent unnecessary reloading
    """
    parser = argparse.ArgumentParser(description='Adding historical recipients to Broker.')
    parser.add_argument('--block_size', '-s', help='Number of rows to batch load', type=int, default=LOAD_BATCH_SIZE)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--reload_file', '-r', action='store_true', help='Reload HistoricDUNS table from file and'
                                                                         ' update from SAM')
    action.add_argument('--update_from_sam', '-u', action='store_true', help='Update the current HistoricDUNS with any'
                                                                             'new columns or updated data')

    args = parser.parse_args()
    reload_file = args.reload_file
    update_from_sam = args.update_from_sam
    block_size = args.block_size

    sess = GlobalDB.db().session

    if reload_file:
        logger.info('Retrieving historical recipients file')
        start = datetime.now()
        if CONFIG_BROKER["use_aws"]:
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            recps_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['archive_bucket'],
                                                                         'Key': "DUNS_export_deduped.csv"},
                                                          ExpiresIn=10000)
        else:
            recps_file = os.path.join(CONFIG_BROKER["broker_files"], "DUNS_export_deduped.csv")

        if not recps_file:
            raise OSError("No DUNS_export_deduped.csv found.")

        logger.info("Retrieved historical recipients file in {} s".format((datetime.now() - start).total_seconds()))

        try:
            run_sam_batches(recps_file, sess, block_size)
        except Exception as e:
            logger.exception(e)
            sess.rollback()
    else:
        # if we're using an old historic recipients table, clean it up before importing
        clean_historic_recipients(sess)

        if update_from_sam:
            reload_from_sam(sess)

    # import the historic recipients to the current SAM table
    import_historic_recipients(sess)

    sess.close()
    logger.info("Updating historical recipients complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
