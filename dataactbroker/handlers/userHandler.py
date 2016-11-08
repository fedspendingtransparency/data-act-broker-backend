import uuid

from dataactcore.interfaces.function_bag import check_permission_by_bit_number, has_permission
from dataactcore.models.userModel import PermissionType
from dataactcore.models.userInterface import UserInterface

from dataactcore.models.lookups import PERMISSION_TYPE_DICT

from dataactcore.interfaces.db import GlobalDB

class UserHandler(UserInterface):
    """ Responsible for all interaction with the user database

    Class Fields:
    dbName -- Name of user database
    credentialsFile -- This file should store a JSON with keys "username" and "password" for the database

    Instance Fields:
    engine -- sqlalchemy engine for creating connections and sessions to database
    connection -- sqlalchemy connection to user database
    session - sqlalchemy session for ORM calls to user database
    """

    def getUserPermissions(self, user):
        """ Get name for specified permissions for this user

        Arguments:
            user
        Returns:
            array of permission names
        """
        sess = GlobalDB.db().session
        all_permissions = sess.query(PermissionType).all()
        user_permissions = []
        for permission in all_permissions:
            if has_permission(user, permission.name):
                user_permissions.append(str(permission.name))
        return sorted(user_permissions, key=str.lower)

    def grantPermission(self,user,permission_name):
        """ Grant a user a permission specified by name, does not affect other permissions

        Arguments:
            user - User object
            permission_name - permission to grant
        """
        if user.permissions is None:
            # Start users with zero permissions
            user.permissions = 0
        bit_number = PERMISSION_TYPE_DICT[permission_name]
        if not check_permission_by_bit_number(user, bit_number):
            # User does not have permission, grant it
            user.permissions += (2 ** bit_number)
            self.session.commit()

    def removePermission(self,user,permission_name):
        """ Remove a permission specified by name from user

        Arguments:
            user - User object
            permissionName - permission to remove
        """
        if user.permissions is None:
            # Start users with zero permissions
            user.permissions = 0
        bit_number = PERMISSION_TYPE_DICT[permission_name]
        if check_permission_by_bit_number(user, bit_number):
            # User has permission, remove it
            user.permissions -= (2 ** bit_number)
            self.session.commit()
