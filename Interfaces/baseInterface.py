import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker

class baseInterface:
    dbConfigFile = None # Should be overwritten by child classes

    def __init__(self):
        # Load config info
        confDict = json.loads(open(self.dbConfigFile,"r").read())
        # Create sqlalchemy connection and session
        self.engine = sqlalchemy.create_engine(username = confDict["username"], password = confDict["password"], host = confDict["host"], port = confDict["port"])
        self.connection = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        session = Session()