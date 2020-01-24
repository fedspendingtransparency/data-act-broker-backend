import logging
import sys
import argparse
import datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactbroker.fsrs import config_valid, fetch_and_replace_batch, GRANT, PROCUREMENT, config_state_mappings
from dataactcore.models.fsrs import FSRSProcurement, FSRSGrant
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Backfill records from FSRS')
    parser.add_argument('-p', '--max_procurement_id', type=int, help="Load procurement awards up to this id")
    parser.add_argument('-g', '--max_grant_id', type=int, help="Load grant awards up to this id")

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
            if args.max_procurement_id:
                logger.info('Reloading existing FSRS reports up to procurement id {}'.format(args.max_procurement_id))
                old_proc_ids = sess.query(FSRSProcurement.id).filter(FSRSProcurement.id < args.max_procurement_id)\
                    .order_by(FSRSProcurement.id).all()
                for proc_id in old_proc_ids:
                    procs = fetch_and_replace_batch(sess, PROCUREMENT, proc_id[0])
            if args.max_grant_id:
                logger.info('Loading FSRS reports up to grant id {}'.format(args.max_grant_id))
                old_grant_ids = sess.query(FSRSGrant.id).filter(FSRSGrant.id < args.max_grant_id)\
                    .order_by(FSRSGrant.id).all()
                for grant_id in old_grant_ids:
                    grants = fetch_and_replace_batch(sess, GRANT, grant_id[0])
