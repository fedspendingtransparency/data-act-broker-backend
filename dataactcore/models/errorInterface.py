from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import Status, ErrorType

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

    def getStatusId(self,statusName):
        """ Get status ID for given name """
        return self.getIdFromDict(Status,"STATUS_DICT","name",statusName,"status_id")

    def getTypeId(self,typeName):
        return self.getIdFromDict(ErrorType,"TYPE_DICT","name",typeName,"error_type_id")
