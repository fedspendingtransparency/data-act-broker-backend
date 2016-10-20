from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.userModel import UserStatus


class UserInterface(BaseInterface):
    """Manages all interaction with the user database."""

    def __init__(self):
        super(UserInterface, self).__init__()

    def getUserStatusId(self, statusName):
        """ Get ID for specified User Status """
        return self.getIdFromDict(
            UserStatus, "STATUS_DICT", "name", statusName, "user_status_id")
