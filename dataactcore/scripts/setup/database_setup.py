import sqlalchemy_utils
import logging
from dataactcore.config import ALEMBIC_PATH, MIGRATION_PATH
from alembic.config import Config
from alembic import command
from sqlalchemy.exc import ProgrammingError
from dataactcore.interfaces.db import db_uri


def create_database(db_name):
    """Create specified database if it doesn't exist."""
    connect_string = db_uri(db_name)
    if not sqlalchemy_utils.database_exists(connect_string):
        sqlalchemy_utils.create_database(connect_string)


def drop_database(db_name):
    """Drop specified database."""
    connect_string = db_uri(db_name)
    if sqlalchemy_utils.database_exists(connect_string):
        sqlalchemy_utils.drop_database(connect_string)


def run_migrations():
    """Run Alembic migrations for a specific database/model set."""
    logging.disable(logging.WARN)
    alembic_cfg = Config(ALEMBIC_PATH)
    alembic_cfg.set_main_option("script_location", MIGRATION_PATH)
    try:
        command.upgrade(alembic_cfg, "head")
    except ProgrammingError as e:
        if "relation" and "already exists" in str(e):
            raise Exception("Cannot run initial db migration if tables "
                            "already exist. " + str(e))
        else:
            raise
    finally:
        logging.disable(logging.NOTSET)
