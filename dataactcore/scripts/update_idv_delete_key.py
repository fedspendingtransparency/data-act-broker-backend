import logging
import time

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

FPDS_IDV_DELETE_KEY_SQL = """
    UPDATE detached_award_procurement
    SET idv_delete_key = CONCAT(COALESCE(agency_id, '-none-'), '_', COALESCE(piid, '-none-'), '_',
                                COALESCE(award_modification_amendme, '-none-'))
    WHERE pulled_from = 'IDV'
        AND idv_delete_key IS NULL
"""

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        start = time.time()
        logger.info("Updating existing FPDS IDV records to have a idv_delete_key")
        sess.execute(FPDS_IDV_DELETE_KEY_SQL)
        sess.commit()
        logger.info("Updated existing FPDS IDV records to have a idv_delete_key, took {} seconds"
                    .format(time.time()-start))

        sess.close()
