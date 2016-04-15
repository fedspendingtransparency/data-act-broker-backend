import sqlalchemy_utils
from dataactcore.config import CONFIG_DB

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
