import sqlalchemy_utils
import logging
from dataactcore.config import ALEMBIC_PATH, MIGRATION_PATH
from alembic.config import Config
from alembic import command
from sqlalchemy.exc import ProgrammingError
from dataactcore.interfaces.db import dbURI


def createDatabase(dbName):
    """Create specified database if it doesn't exist."""
    connectString = dbURI(dbName)
    if not sqlalchemy_utils.database_exists(connectString):
        sqlalchemy_utils.create_database(connectString)


def dropDatabase(dbName):
    """Drop specified database."""
    connectString = dbURI(dbName)
    if sqlalchemy_utils.database_exists(connectString):
        sqlalchemy_utils.drop_database(connectString)


def runMigrations():
    """Run Alembic migrations for a specific database/model set.

    Args:
        alembicDbName: the database to target (must match one of the
        default databases in alembic.ini.
    """
    logging.disable(logging.WARN)
    alembic_cfg = Config(ALEMBIC_PATH)
    alembic_cfg.set_main_option("script_location", MIGRATION_PATH)
    try:
        command.upgrade(alembic_cfg, "head")
    except ProgrammingError as e:
        if "relation" and "already exists" in e.message:
            raise Exception("Cannot run initial db migration if tables "
                            "already exist. " + e.message)
        else:
            raise
    finally:
        logging.disable(logging.NOTSET)
