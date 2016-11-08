from dataactcore.models.baseInterface import BaseInterface

from dataactcore.models.lookups import USER_STATUS_DICT

class UserInterface(BaseInterface):
    """Manages all interaction with the user database."""

    def __init__(self):
        super(UserInterface, self).__init__()

    def getUserStatusId(self, status_name):
        """ Get ID for specified User Status """
        return USER_STATUS_DICT[status_name]
