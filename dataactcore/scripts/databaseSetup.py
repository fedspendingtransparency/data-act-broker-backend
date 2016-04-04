import sqlalchemy
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from dataactcore.config import CONFIG_DB


def createDatabase(dbName):
    """Create specified database if it doesn't exist."""
    connectString = "postgresql://{}:{}@{}:{}/{}".format(CONFIG_DB["username"],
        CONFIG_DB["password"], CONFIG_DB["host"], CONFIG_DB["port"],
        dbName)
    db = sqlalchemy.create_engine(
        connectString, isolation_level="AUTOCOMMIT")
    try:
        connect = db.connect()
        try:
            connect.execute(CreateSchema('public'))
        except ProgrammingError as e:
            if "already exists" in e.message:
                # database schema is already present, so
                # nothing to see here
                pass
    except OperationalError as e:
        # Database doesn't exist, so create it
        connectString = connectString.replace(dbName, CONFIG_DB["base_db_name"])
        db = sqlalchemy.create_engine(
            connectString, isolation_level="AUTOCOMMIT")
        connect = db.connect()
        connect.execute(
            "CREATE DATABASE {}".format(dbName))


