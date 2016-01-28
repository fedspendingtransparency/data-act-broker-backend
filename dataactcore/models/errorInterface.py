from dataactcore.models.baseInterface import BaseInterface

class ErrorInterface(BaseInterface):
    """ Manages communication with error database """
    dbName = "error_data"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(ErrorInterface,self).__init__()
        #Base.metadata.bind = self.engine
        #Base.metadata.create_all(self.engine)


    @staticmethod
    def getDbName():
        """ Return database name"""
        return ErrorInterface.dbName

    def getSession(self):
        return self.session