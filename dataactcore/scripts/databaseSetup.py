import sqlalchemy_utils
import logging
from dataactcore.config import CONFIG_DB, ALEMBIC_PATH, MIGRATION_PATH
from alembic.config import Config
from alembic import command
from sqlalchemy.exc import ProgrammingError


def createDatabase(dbName):
    """Create specified database if it doesn't exist."""
    config = CONFIG_DB
    connectString = "postgresql://{}:{}@{}:{}/{}".format(config["username"],
        config["password"], config["host"], config["port"],
        dbName)

    if not sqlalchemy_utils.database_exists(connectString):
        sqlalchemy_utils.create_database(connectString)


def dropDatabase(dbName):
    """Drop specified database."""
    config = CONFIG_DB
    connectString = "postgresql://{}:{}@{}:{}/{}".format(config["username"],
        config["password"], config["host"], config["port"], dbName)
    if sqlalchemy_utils.database_exists(connectString):
        sqlalchemy_utils.drop_database(connectString)


def runMigrations(alembicDbName):
    """Run Alembic migrations for a specific database/model set.

    Args:
        alembicDbName: the database to target (must match one of the
        default databases in alembic.ini.
    """
    logging.disable(logging.WARN)
    alembic_cfg = Config(ALEMBIC_PATH)
    alembic_cfg.set_main_option("script_location", MIGRATION_PATH)
    alembic_cfg.set_main_option("databases", alembicDbName)
    try:
        command.upgrade(alembic_cfg, "head")
    except ProgrammingError as e:
        if "relation" and "already exists" in e.message:
            raise Exception("Cannot run initial db migration if tables "
                            "already exist. " + e.message)
    finally:
        logging.disable(logging.NOTSET)