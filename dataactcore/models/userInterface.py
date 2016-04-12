from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.userModel import UserStatus
from dataactcore.config import CONFIG_DB


class UserInterface(BaseInterface):
    """Manages all interaction with the user database."""
    dbName = CONFIG_DB['user_db_name']
    dbConfig = CONFIG_DB
    Session = None
    engine = None
    session = None

    def __init__(self):
        super(UserInterface, self).__init__()

    @staticmethod
    def getDbName():
        """Return database name."""
        return UserInterface.dbName

    def getUserStatusId(self, statusName):
        return self.getIdFromDict(
            UserStatus, "STATUS_DICT", "name", statusName, "user_status_id")
