from dataactcore.models.baseInterface import BaseInterface

class StagingInterface(BaseInterface):
    """ Manages all interaction with the validation database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """

    dbName = "staging"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(StagingInterface,self).__init__()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return StagingInterface.dbName
