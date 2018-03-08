import logging
import sys
import argparse

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactbroker.fsrs import config_valid, fetch_and_replace_batch, GRANT, PROCUREMENT
from dataactvalidator.health_check import create_app


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(description='Pull data from FSRS Feed')
    parser.add_argument('-p', '--procurement',action='store_true', help="Load just procurement awards")
    parser.add_argument('-g', '--grants', action='store_true', help="Load just grant awards")
    parser.add_argument('-i', '--ids', type=int, nargs='+',
                        help="Single or list of FSRS ids to pull from the FSRS API ")

    with create_app().app_context():
        sess = GlobalDB.db().session
        if not config_valid():
            logger.error("No config for broker/fsrs/[service]/wsdl")
            sys.exit(1)
        else:
            args = parser.parse_args()

            # Regular FSRS data load, starts where last load left off
            if len(sys.argv) <= 1:
                awards = ['Starting']
                while len(awards) > 0:
                    procs = fetch_and_replace_batch(sess, PROCUREMENT)
                    grants = fetch_and_replace_batch(sess, GRANT)
                    awards = procs + grants
                    numSubAwards = sum(len(a.subawards) for a in awards)
                    logger.info("Inserted/Updated %s awards, %s subawards", len(awards), numSubAwards)

            elif args.procurement and args.ids:
                for procurement_id in args.ids:
                    procs = fetch_and_replace_batch(sess, PROCUREMENT, procurement_id)
                    numSubAwards = sum(len(a.subawards) for a in procs)
                    logger.info("Inserted/Updated %s awards, %s subawards", len(procs), numSubAwards)
            elif args.grants and args.ids:
                for grant_id in args.ids:
                    grants = fetch_and_replace_batch(sess, GRANT, grant_id)
                    numSubAwards = sum(len(a.subawards) for a in grants)
                    logger.info("Inserted/Updated %s awards, %s subawards", len(grants), numSubAwards)
            else:
                if not args.ids:
                    logger.error('Missing --ids argument when loading just procurment or grants awards')
                else:
                    logger.error('Missing --procurement or --grants argument when loading specific award ids')

