import argparse
import logging
import os

from flask_bcrypt import Bcrypt


from dataactbroker.scripts.setupEmails import setup_emails
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import create_user_with_password
from dataactcore.logging import configure_logging
from dataactcore.models.userModel import User
from dataactcore.scripts.setupAllDB import setup_all_db
from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.scripts.loadFile import load_domain_values
from dataactvalidator.scripts.load_sf133 import load_all_sf133
from dataactvalidator.scripts.loadTas import load_tas
from dataactvalidator.scripts.loadLocationData import load_location_data

logger = logging.getLogger(__name__)
basePath = CONFIG_BROKER["path"]
validator_config_path = os.path.join(basePath, "dataactvalidator", "config")


def setup_db():
    """Set up broker database and initialize data."""
    logger.info('Setting up db')
    setup_all_db()
    setup_emails()


def create_admin():
    """Create initial admin user."""
    logger.info('Creating admin user')
    admin_email = CONFIG_BROKER['admin_email']
    admin_pass = CONFIG_BROKER['admin_password']
    with create_app().app_context():
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.email == admin_email).one_or_none()
        if not user:
            # once the rest of the setup scripts are updated to use
            # GlobalDB instead of databaseSession, move the app_context
            # creation up to initialize()
            user = create_user_with_password(admin_email, admin_pass, Bcrypt(), website_admin=True)
    return user


def load_tas_lookup():
    """Load/update the TAS table to reflect the latest list."""
    logger.info('Loading TAS')
    load_tas()


def load_sql_rules():
    """Load the SQL-based validation rules."""
    logger.info('Loading SQL-based validation rules')
    SQLLoader.load_sql("sqlRules.csv")


def load_domain_value_files(base_path):
    """Load domain values (e.g., CGAC codes, object class, SF-133)."""
    logger.info('Loading domain values')
    load_domain_values(base_path)


def load_sf133():
    logger.info('Loading SF-133')
    # Unlike other domain value files, SF 133 data is stored
    # on S3. If the application's 'use_aws' option is turned
    # off, tell the SF 133 load to look for files in the
    # validator's local config file instead
    if CONFIG_BROKER['use_aws']:
        load_all_sf133()
    else:
        load_all_sf133(validator_config_path)


def load_validator_schema():
    """Load file-level .csv schemas into the broker database."""
    logger.info('Loading validator schemas')
    SchemaLoader.load_all_from_path(validator_config_path)


def load_location_codes():
    """Load city and county codes into the broker database."""
    logger.info('Loading location data')
    load_location_data()


def main():
    parser = argparse.ArgumentParser(description='Initialize the DATA Act Broker.')
    parser.add_argument('-i', '--initialize', help='Run all broker initialization tasks', action='store_true')
    parser.add_argument('-db', '--setup_db', help='Create broker database and helper tables', action='store_true')
    parser.add_argument('-a', '--create_admin', help='Create an admin user', action='store_true')
    parser.add_argument('-r', '--load_rules', help='Load SQL-based validation rules', action='store_true')
    parser.add_argument('-d', '--update_domain', help='load slowly changing domain values such s object class',
                        action='store_true')
    parser.add_argument('-t', '--update_tas', help='Update broker TAS list', action='store_true')
    parser.add_argument('-s', '--update_sf133', help='Update broker SF-133 reports', action='store_true')
    parser.add_argument('-v', '--update_validator', help='Update validator schema', action='store_true')
    parser.add_argument('-l', '--load_location', help='Load city and county codes', action='store_true')
    args = parser.parse_args()

    if args.initialize:
        setup_db()
        load_sql_rules()
        load_domain_value_files(validator_config_path)
        load_tas_lookup()
        load_sf133()
        load_validator_schema()
        load_location_codes()
        return

    if args.setup_db:
        setup_db()

    if args.create_admin:
        create_admin()

    if args.load_rules:
        load_sql_rules()

    if args.update_domain:
        load_domain_value_files(validator_config_path)

    if args.update_tas:
        load_tas_lookup()

    if args.update_sf133:
        load_sf133()

    if args.update_validator:
        load_validator_schema()

    if args.load_location:
        load_location_codes()

if __name__ == '__main__':
    configure_logging()
    main()
