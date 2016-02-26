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
        if(Status.STATUS_DICT == None or (len(Status.STATUS_DICT)==0)):
            Status.STATUS_DICT = {}
            # Pull status values out of DB
            queryResult = self.session.query(Status).all()

            for status in queryResult:
                Status.STATUS_DICT[status.name] = status.status_id

        if(not statusName in Status.STATUS_DICT):
            open("statusError.log","a").write("Not a valid file status: " + statusName + ", dict is: " + str(Status.STATUS_DICT))
            raise ValueError("Not a valid file status: " + statusName + ", dict is: " + str(Status.STATUS_DICT))
        return Status.STATUS_DICT[statusName]

    def getTypeId(self,typeName):
        if(ErrorType.TYPE_DICT == None):
            ErrorType.TYPE_DICT = {}
            # Pull status values out of DB:
            queryResult = self.session.query(ErrorType).all()

            for type in queryResult:
                ErrorType.TYPE_DICT[type.name] = type.error_type_id

        if(not typeName in ErrorType.TYPE_DICT):
            raise ValueError("Not a valid error type: " + typeName)
        return ErrorType.TYPE_DICT[typeName]