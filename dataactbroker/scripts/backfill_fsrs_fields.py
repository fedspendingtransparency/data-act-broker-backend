import logging
import sys
import argparse
import datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactbroker.fsrs import config_valid, fetch_and_replace_batch, GRANT, PROCUREMENT, config_state_mappings
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Backfill records from FSRS')
    parser.add_argument('-min_p', '--min_procurement_id', type=int, default=0,
                        help="Load procurement awards from this id")
    parser.add_argument('-min_g', '--min_grant_id', type=int, default=0,
                        help="Load grant awards from this id")
    parser.add_argument('-max_p', '--max_procurement_id', type=int, help="Load procurement awards up to this id")
    parser.add_argument('-max_g', '--max_grant_id', type=int, help="Load grant awards up to this id")

    with create_app().app_context():
        logger.info("Begin backilling FSRS data from FSRS API")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        # Setups state mapping for deriving state name
        config_state_mappings(sess, init=True)

        if not config_valid():
            logger.error("No config for broker/fsrs/[service]/wsdl")
            sys.exit(1)
        elif not(args.max_procurement_id or args.max_grant_id):
            logger.error("Must run one either/both of procurement and grant backfills")
            sys.exit(1)
        else:
            next_proc_id = args.min_procurement_id
            max_proc_id = args.max_procurement_id if args.max_procurement_id else 0
            next_grant_id = args.min_grant_id
            max_grant_id = args.max_grant_id if args.max_grant_id else 0

            if max_grant_id and (max_grant_id - next_grant_id) < 1 or next_grant_id < 0:
                logger.error("Min grant must be positive and less than max grant.")
                sys.exit(1)
            if max_proc_id and (max_proc_id - next_proc_id) < 1 or next_proc_id < 0:
                logger.error("Min procurement must be positive and less than max procurement.")
                sys.exit(1)

            if max_proc_id:
                logger.info('Loading FSRS contracts from {} up to {}'.format(next_proc_id, max_proc_id))
            if max_grant_id:
                logger.info('Loading FSRS grants from {} up to {}'.format(next_grant_id, max_grant_id))

            awards = ['Starting']
            while len(awards) > 0:
                procs = []
                if max_proc_id:
                    procs = fetch_and_replace_batch(sess, PROCUREMENT, next_proc_id, min_id=True, max_id=max_proc_id)
                    if procs:
                        next_proc_id = procs[-1].id + 1
                grants = []
                if max_grant_id:
                    grants = fetch_and_replace_batch(sess, GRANT, next_grant_id, min_id=True, max_id=max_grant_id)
                    if grants:
                        next_grant_id = grants[-1].id + 1
                awards = procs + grants
        logger.info('Finished FSRS backfill.')
