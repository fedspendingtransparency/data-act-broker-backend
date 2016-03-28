from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import Status, ErrorType
from dataactcore.config import CONFIG_DB


class ErrorInterface(BaseInterface):
    """Manages communication with error database."""
    dbName = CONFIG_DB['error_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        super(ErrorInterface,self).__init__()
        #Base.metadata.bind = self.engine
        #Base.metadata.create_all(self.engine)

    @staticmethod
    def getDbName():
        """Return database name."""
        return ErrorInterface.dbName

    def getSession(self):
        return self.session

    def getStatusId(self,statusName):
        """Get status ID for given name."""
        return self.getIdFromDict(
            Status, "STATUS_DICT", "name", statusName, "status_id")

    def getTypeId(self,typeName):
        return self.getIdFromDict(
            ErrorType, "TYPE_DICT", "name", typeName, "error_type_id")
