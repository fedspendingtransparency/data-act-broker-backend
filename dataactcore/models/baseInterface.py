import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker

class BaseInterface:
    """ Abstract base interface to be inherited by interfaces for specific databases
    """

    dbConfigFile = None # Should be overwritten by child classes
    dbName = None # Should be overwritten by child classes

    def __init__(self):
        if(self.dbConfigFile == None or self.dbName == None):
            # Child class needs to set these before calling base constructor
            raise ValueError("Need dbConfigFile and dbName defined")
        # Load config info
        confDict = json.loads(open(self.dbConfigFile,"r").read())
        # Create sqlalchemy connection and session
        self.engine = sqlalchemy.create_engine("postgresql://" + confDict["username"] + ":" + confDict["password"] + "@" + confDict["host"] + ":" + confDict["port"] + "/" + self.dbName)
        self.connection = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()