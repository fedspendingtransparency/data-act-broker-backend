import argparse
import logging
import os

from flask_bcrypt import Bcrypt

from dataactvalidator.app import createApp
from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.models.userModel import User
from dataactcore.interfaces.function_bag import createUserWithPassword
from dataactcore.scripts.setupAllDB import setupAllDB
from dataactbroker.handlers.aws.session import SessionTable
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
from dataactvalidator.scripts.loadTas import loadTas
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.loadFile import loadDomainValues
from dataactvalidator.scripts.loadSf133 import loadAllSf133

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
basePath = CONFIG_BROKER["path"]
validator_config_path = os.path.join(basePath, "dataactvalidator", "config")


def setupDB():
    """Set up broker database and initialize data."""
    logger.info('Setting up databases')
    setupAllDB()
    setupEmails()


def createAdmin():
    """Create initial admin user."""
    logger.info('Creating admin user')
    adminEmail = CONFIG_BROKER['admin_email']
    adminPass = CONFIG_BROKER['admin_password']
    with createApp().app_context():
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.email == adminEmail).one_or_none()
        if not user:
            # once the rest of the setup scripts are updated to use
            # GlobalDB instead of databaseSession, move the app_context
            # creation up to initialize()
            user = createUserWithPassword(
                adminEmail, adminPass, Bcrypt(), permission=2)
    return user


def setupSessionTable():
    """Create Dynamo session table."""
    logger.info('Setting up DynamoDB session table')
    SessionTable.createTable(CONFIG_BROKER['local'], CONFIG_DB['dynamo_port'])


def loadTasLookup():
    """Load/update the TAS table to reflect the latest list."""
    logger.info('Loading TAS')
    loadTas()


def loadSqlRules():
    """Load the SQL-based validation rules."""
    logger.info('Loading SQL-based validation rules')
    SQLLoader.loadSql("sqlRules.csv")


def loadDomainValueFiles(basePath):
    """Load domain values (e.g., CGAC codes, object class, SF-133)."""
    logger.info('Loading domain values')
    loadDomainValues(basePath)


def loadSf133():
    logger.info('Loading SF-133')
    # Unlike other domain value files, SF 133 data is stored
    # on S3. If the application's 'use_aws' option is turned
    # off, tell the SF 133 load to look for files in the
    # validator's local config file instead
    if CONFIG_BROKER['use_aws']:
        loadAllSf133()
    else:
        loadAllSf133(validator_config_path)


def loadValidatorSchema():
    """Load file-level .csv schemas into the broker database."""
    logger.info('Loading validator schemas')
    SchemaLoader.loadAllFromPath(validator_config_path)


def main():
    parser = argparse.ArgumentParser(description='Initialize the DATA Act Broker.')
    parser.add_argument('-i', '--initialize', help='Run all broker initialization tasks', action='store_true')
    parser.add_argument('-db', '--setup_db', help='Create broker database and helper tables', action='store_true')
    parser.add_argument('-a', '--create_admin', help='Create an admin user', action='store_true')
    parser.add_argument('-r', '--load_rules', help='Load SQL-based validation rules', action='store_true')
    parser.add_argument('-d', '--update_domain', help='load slowly changing domain values such s object class', action='store_true')
    parser.add_argument('-t', '--update_tas', help='Update broker TAS list', action='store_true')
    parser.add_argument('-s', '--update_sf133', help='Update broker SF-133 reports', action='store_true')
    parser.add_argument('-v', '--update_validator', help='Update validator schema', action='store_true')
    args = parser.parse_args()

    if args.initialize:
        setupAllDB()
        setupEmails()
        setupSessionTable()
        loadSqlRules()
        loadDomainValueFiles(validator_config_path)
        loadTas()
        loadSf133()
        loadValidatorSchema()
        return

    if args.setup_db:
        logger.info('Setting up databases')
        setupAllDB()
        setupEmails()
        setupSessionTable()

    if args.create_admin:
        createAdmin()

    if args.load_rules:
        loadSqlRules()

    if args.update_domain:
        loadDomainValueFiles(validator_config_path)

    if args.update_tas:
        loadTas()

    if args.update_sf133:
        loadSf133()

    if args.update_validator:
        loadValidatorSchema()

if __name__ == '__main__':
    main()

