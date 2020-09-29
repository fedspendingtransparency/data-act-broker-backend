import argparse
import logging
import os

from dataactcore.config import CONFIG_DB
from dataactcore.logging import configure_logging
from dataactcore.scripts.database_setup import create_database, run_migrations
from dataactcore.scripts.setup_error_db import setup_error_db
from dataactcore.scripts.setup_job_tracker_db import setup_job_tracker_db
from dataactcore.scripts.setup_user_db import setup_user_db
from dataactcore.scripts.setup_validation_db import setup_validation_db
from dataactcore.scripts.setup_static_data import setup_static_data
from dataactcore.scripts.setup_submission_type_db import setup_submission_type_db


logger = logging.getLogger(__name__)


def setup_all_db(db_name=None, no_data=False):
    """Sets up all databases"""
    logger.info("Invoking setup_all_db with db_name={} and no_data={}".format(db_name, no_data))
    if db_name:
        # Ensure the config is set to setup the specified db
        CONFIG_DB['db_name'] = db_name
    create_database(CONFIG_DB['db_name'])
    logger.info("Created database (if not existing) {}".format(CONFIG_DB['db_name']))
    logger.info("Running migrations in database {}".format(CONFIG_DB['db_name']))
    run_migrations()

    if not no_data:
        logger.info("Setting up baseline data in database {}".format(CONFIG_DB['db_name']))
        setup_job_tracker_db()
        setup_error_db()
        setup_user_db()
        setup_validation_db()
        setup_static_data()
        setup_submission_type_db()


if __name__ == '__main__':
    configure_logging()

    logger.info("Running script {}".format(os.path.basename(__file__)))

    parser = argparse.ArgumentParser(description='Setup a DATA Act Broker database.')
    parser.add_argument('--db-name', '--dbname', help='Setup a database with this name', action='store')
    parser.add_argument('--no-data', help='Leave the database empty of data', action='store_true')
    setup_all_db(**vars(parser.parse_args()))
