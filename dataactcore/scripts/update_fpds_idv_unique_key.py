import logging
import time
import datetime

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import DetachedAwardProcurement

from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter

logger = logging.getLogger(__name__)

FPDS_DELETE_DUPLICATE_NEW_KEY = """
    WITH duplicate_count AS (SELECT
            CONCAT(COALESCE(agency_id, '-none-'), '_', COALESCE(piid, '-none-'), '_',
                COALESCE(award_modification_amendme, '-none-')) AS idv_delete_key,
            last_modified
        FROM detached_award_procurement
        WHERE pulled_from = 'IDV'),
    max_values AS (SELECT
        idv_delete_key,
        MAX(last_modified) AS max_mod
    FROM
        duplicate_count
    GROUP BY idv_delete_key),
    delete_with_mod AS (SELECT dc.idv_delete_key, dc.last_modified
    FROM max_values AS mv
        JOIN duplicate_count AS dc ON dc.idv_delete_key = mv.idv_delete_key
    WHERE mv.max_mod != dc.last_modified)
    DELETE FROM detached_award_procurement AS dap
    USING delete_with_mod AS dwm
    WHERE CONCAT(COALESCE(dap.agency_id, '-none-'), '_', COALESCE(dap.piid, '-none-'), '_',
                COALESCE(dap.award_modification_amendme, '-none-')) = dwm.idv_delete_key
        AND dap.last_modified = dwm.last_modified
        AND dap.pulled_from = 'IDV'
"""

FPDS_UNIQUE_KEY_UPDATE_SQL = """
    UPDATE detached_award_procurement
    SET detached_award_proc_unique = CONCAT(COALESCE(agency_id, '-none-'), '_-none-_', COALESCE(piid, '-none-'), '_',
                                            COALESCE(award_modification_amendme, '-none-'), '_-none-_-none-'),
        updated_at = '{}'
    WHERE pulled_from = 'IDV'
"""


def write_idvs_to_file():
    """ Get a list of all IDVs and write them to a delete file in the fpds_delete_bucket. This is because we need
        to clean out the website of IDV records so everything is new and not duplicated when we pull it in.
    """
    start = time.time()
    logger.info("Writing IDV delete file for website")
    # Get all IDVs (only the ID and unique key)
    all_idvs = sess.query(DetachedAwardProcurement.detached_award_procurement_id,
                          DetachedAwardProcurement.detached_award_proc_unique).filter_by(pulled_from="IDV")
    now = datetime.datetime.now()
    seconds = int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    file_name = now.strftime('%m-%d-%Y') + "_delete_records_IDV_" + str(seconds) + ".csv"
    headers = ["detached_award_procurement_id", "detached_award_proc_unique"]
    # Writing files
    if CONFIG_BROKER["use_aws"]:
        with CsvS3Writer(CONFIG_BROKER['aws_region'], CONFIG_BROKER['fpds_delete_bucket'], file_name,
                         headers) as writer:
            for idv in all_idvs:
                writer.write([idv.detached_award_procurement_id, idv.detached_award_proc_unique])
            writer.finish_batch()
    else:
        with CsvLocalWriter(file_name, headers) as writer:
            for idv in all_idvs:
                writer.write([idv.detached_award_procurement_id, idv.detached_award_proc_unique])
            writer.finish_batch()

    logger.info("Wrote IDV delete file for website, took {} seconds"
                .format(time.time() - start))

    # Deleting to free up space in case it doesn't auto-delete after this function closes
    del all_idvs


def delete_duplicate_idvs():
    """ Delete all IDV rows that would be duplicates using the new unique key setup for IDVs. Keep only the newest
        version of each unique key
    """
    start = time.time()
    logger.info("Updating existing FPDS IDV records to have the proper unique key")
    sess.execute(FPDS_DELETE_DUPLICATE_NEW_KEY)
    sess.commit()
    logger.info("Updated existing FPDS IDV records to have the proper unique key, took {} seconds"
                .format(time.time() - start))


def update_unique_keys():
    """ Update the detached_award_proc_unique key for IDV records """
    start = time.time()
    now = datetime.datetime.utcnow()
    logger.info("Updating existing FPDS IDV records to have the proper unique key")
    sess.execute(FPDS_UNIQUE_KEY_UPDATE_SQL.format(now.strftime('%m/%d/%Y %H:%M:%S')))
    sess.commit()
    logger.info("Updated existing FPDS IDV records to have the proper unique key, took {} seconds"
                .format(time.time() - start))

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        # Create file with all IDVs so website can delete
        write_idvs_to_file()

        # Delete older IDVs that would be duplicates with new unique key
        delete_duplicate_idvs()

        # Update all unique keys for IDVs
        update_unique_keys()

        sess.close()
