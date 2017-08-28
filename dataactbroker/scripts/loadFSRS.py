import logging
import sys

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactbroker.fsrs import config_valid, fetch_and_replace_batch, GRANT, PROCUREMENT
from dataactvalidator.health_check import create_app


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        sess = GlobalDB.db().session
        if not config_valid():
            logger.error("No config for broker/fsrs/[service]/wsdl")
            sys.exit(1)
        else:
            procs = fetch_and_replace_batch(sess, PROCUREMENT)
            grants = fetch_and_replace_batch(sess, GRANT)
            awards = procs + grants
            numSubAwards = sum(len(a.subawards) for a in awards)
            logger.info("Inserted/Updated %s awards, %s subawards", len(awards), numSubAwards)
