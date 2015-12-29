from dataactcore.models.baseInterface import BaseInterface
from errorModels import Base

class ErrorInterface(BaseInterface):
    dbName = "error_data"
    credFileName = "dbCred.json"

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(ErrorInterface,self).__init__()
        Base.metadata.bind = self.engine
        Base.metadata.create_all(self.engine)


    @staticmethod
    def getDbName():
        """ Return database name"""
        return ErrorInterface.dbName

    def getSession(self):
        return self.session