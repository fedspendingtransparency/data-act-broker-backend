import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app
from dataactcore.utils.duns import backfill_uei
from dataactcore.models.domainModels import DUNS

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        affected = 0

        logger.info('Backfilling empty uei and ultimate_parent_uei in the DUNS table using the API.')
        backfill_uei(sess, DUNS)
        logger.info('Backfill completed')

        sess.close()
