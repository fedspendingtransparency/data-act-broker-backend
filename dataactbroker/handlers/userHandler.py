import uuid
import time

from dataactcore.interfaces.function_bag import check_permission_by_bit_number, has_permission
from dataactcore.models.userModel import User, PermissionType
from dataactcore.models.userInterface import UserInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.models.lookups import USER_STATUS_DICT, PERMISSION_TYPE_DICT

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
    HASH_ROUNDS = 12 # How many rounds to use for hashing passwords

    def getUsers(self, cgac_code=None, status="all", only_active=False):
        """ Return all users in the database """
        query = self.session.query(User)
        if cgac_code is not None:
            query = query.filter(User.cgac_code == cgac_code)
        if status != "all":
            status_id = USER_STATUS_DICT[status]
            query = query.filter(User.user_status_id == status_id)
        if only_active:
            query = query.filter(User.is_active == True)
        return query.all()

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

    def hasPermission(self, user, permission_name):
        """ Checks if user has specified permission

        Arguments:
            user - User object
            permission_name - permission to check
        Returns:
            True if user has the specified permission, False otherwise
        """
        # Get the bit number corresponding to this permission from the PERMISSION_TYPE_DICT and use it to check whether
        # user has the specified permission
        if check_permission_by_bit_number(user, PERMISSION_TYPE_DICT[permission_name]):
            return True
        return False

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

    def checkPassword(self,user,password,bcrypt):
        """ Given a user object and a password, verify that the password is correct.

        Arguments:
            user - User object
            password - Password to check
            bcrypt - bcrypt to use for password hashing
        Returns:
             True if valid password, False otherwise.
        """
        if password is None or password.strip()=="":
            # If no password or empty password, reject
            return False

        # Check the password with bcrypt
        return bcrypt.check_password_hash(user.password_hash,password+user.salt)

    def setPassword(self,user,password,bcrypt):
        """ Given a user and a new password, changes the hashed value in the database to match new password.

        Arguments:
            user - User object
            password - password to be set
            bcrypt - bcrypt to use for password hashing
        Returns:
             True if successful
        """
        # Generate hash with bcrypt and store it
        newSalt =  uuid.uuid4().hex
        user.salt = newSalt
        hash = bcrypt.generate_password_hash(password+newSalt,UserHandler.HASH_ROUNDS)
        user.password_hash = hash.decode("utf-8")
        self.session.commit()
        return True
