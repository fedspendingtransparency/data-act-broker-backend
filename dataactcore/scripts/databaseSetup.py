import sqlalchemy
from sqlalchemy.exc import OperationalError
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
    except OperationalError as e:
        # Database doesn't exist, so create it
        connectString = connectString.replace(dbName, CONFIG_DB["base_db_name"])
        db = sqlalchemy.create_engine(
            connectString, isolation_level="AUTOCOMMIT")
        connect = db.connect()
        connect.execute(
            "CREATE DATABASE {}".format(dbName))

