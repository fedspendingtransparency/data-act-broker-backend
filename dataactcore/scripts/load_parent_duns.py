import argparse
import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.utils.parentDuns import sam_config_is_valid, get_duns_batches, update_missing_parent_names
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        parser = argparse.ArgumentParser(description='Update parent duns columns in DUNS table')
        parser.add_argument('-b', '--batch_start', help='Batch to start with (type int)', type=int, default=0)
        parser.add_argument('-e', '--batch_end', help='Batch to end with (type int)', type=int)
        parser.add_argument('-n', '--parent_name', help='Derives parent name at the end', action='store_true')

        args = parser.parse_args()

        # Parse argument to do load on certain update date
        # Possible option if want to do make sure items load
        sess = GlobalDB.db().session

        if args.parent_name:
            # Derive missing parent names when a parent DUNS number is provided
            update_missing_parent_names(sess)

        else:

            logger.info('Begin loading parents duns to DUNS table')

            # Check to make sure config is valid
            client = sam_config_is_valid()

            # Run updates on DUNS table to retrieve parent DUNS data
            get_duns_batches(client, sess, args.batch_start, args.batch_end)

            logger.info('Finished loading parents duns to DUNS table')

            sess.close()
