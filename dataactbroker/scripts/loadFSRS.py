import logging
import sys

from dataactcore.interfaces.db import databaseSession
from dataactbroker.fsrs import (
    configValid, fetchAndReplaceBatch, GRANT, PROCUREMENT)


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    with databaseSession() as sess:
        if not configValid():
            logger.error("No config for broker/fsrs/[service]/wsdl")
            sys.exit(1)
        else:
            procs = fetchAndReplaceBatch(sess, PROCUREMENT)
            grants = fetchAndReplaceBatch(sess, GRANT)
            awards = procs + grants
            numSubAwards = sum(len(a.subawards) for a in awards)
            logger.info("Inserted/Updated %s awards, %s subawards",
                        len(awards), numSubAwards)
