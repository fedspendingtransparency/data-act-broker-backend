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

FPDS_IDV_DELETE_KEY_SQL = """
    UPDATE detached_award_procurement
    SET detached_award_proc_unique = CONCAT(COALESCE(agency_id, '-none-'), '_-none-_', COALESCE(piid, '-none-'), '_',
                                            COALESCE(award_modification_amendme, '-none-'), '_-none-_-none-'),
        updated_at = {}
    WHERE pulled_from = 'IDV'
"""


def write_idvs_to_file():
    """ Get a list of all IDVs and write them to a delete file in the fpds_delete_bucket. This is because we need
        to clean out the website of IDV records so everything is new and not duplicated when we pull it in.
    """
    # writing the file
    all_idvs = sess.query(DetachedAwardProcurement.detached_award_procurement_id,
                          DetachedAwardProcurement.detached_award_proc_unique).filter_by(pulled_from="IDV")
    now = datetime.datetime.now()
    seconds = int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    file_name = now.strftime('%m-%d-%Y') + "_delete_records_IDV_" + str(seconds) + ".csv"
    headers = ["detached_award_procurement_id", "detached_award_proc_unique"]
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

    # Deleting to free up space in case it doesn't auto-delete after this function closes
    del all_idvs


def update_unique_keys():
    """ Update the detached_award_proc_unique key for IDV records """
    start = time.time()
    logger.info("Updating existing FPDS IDV records to have the proper unique key")
    sess.execute(FPDS_IDV_DELETE_KEY_SQL.format(start))
    sess.commit()
    logger.info("Updated existing FPDS IDV records to have the proper unique key, took {} seconds"
                .format(time.time() - start))

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        write_idvs_to_file()

        update_unique_keys()

        sess.close()
