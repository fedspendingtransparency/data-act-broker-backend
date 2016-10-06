import uuid
import time
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from dataactcore.interfaces.function_bag import checkPermissionByBitNumber
from dataactcore.models.userModel import User, PermissionType
from dataactcore.models.userInterface import UserInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.models.userModel import EmailToken, EmailTemplateType, EmailTemplate

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

    def getTokenSalt(self,token):
        """ gets the salt from a given token so it can be decoded

        Arguments:
            token - Token to extract salt from
        Returns:
            salt for this token
        """
        return  self.session.query(EmailToken.salt).filter(EmailToken.token == token).one()

    def deleteToken(self,token):
        """ deletes old token

        Arguments:
            token - Token to be deleted
        """
        oldToken = self.session.query(EmailToken).filter(EmailToken.token == token).one()
        self.session.delete(oldToken)
        self.session.commit()

    def getUsers(self, cgac_code=None, status="all", only_active=False):
        """ Return all users in the database """
        query = self.session.query(User)
        if cgac_code is not None:
            query = query.filter(User.cgac_code == cgac_code)
        if status != "all":
            status_id = self.getUserStatusId(status)
            query = query.filter(User.user_status_id == status_id)
        if only_active:
            query = query.filter(User.is_active == True)
        return query.all()

    def getUserByUID(self,uid):
        """ Return a User object that matches specified uid

        Arguments:
            uid - User ID to get User object for
        Returns:
            User ORM object for this ID
        """
        query = self.session.query(User).filter(User.user_id == uid)
        # Raise exception if we did not find exactly one user
        result = self.runUniqueQuery(query,"No users with that uid", "Multiple users with that uid")
        return result


    def createUser(self, username):
        """ Creates a new entry for new usernames, if a user is found with this email that has not yet registered, just returns that user's ID.  Raises an exception if multiple results found, or if user has already registered.

        Arguments:
        username - username to find or create an id for
        Returns:
        user object
        """
        # Check if user exists
        queryResult = self.session.query(User).options(joinedload("user_status")).filter(User.username == username).all()
        if(len(queryResult) == 1):
            # If so, check their status
            user = queryResult[0]
            if(user.user_status.name == "awaiting_confirmation" or user.user_status.name == "email_confirmed"):
                # User has not yet registered, may restart process
                return user
        elif(len(queryResult) == 0):
            # If not, add new user
            newUser = User(username = username)
            self.session.add(newUser)
            self.session.commit()
            return newUser
        else:
            # Multiple entries for this user, server error
            raise MultipleResultsFound("Multiple entries for single username")

    def getUserByEmail(self,email):
        """ Return a User object that matches specified email

        Arguments:
            email - email to search for
        Returns:
            User object with that email, raises exception if none found
        """
        query = self.session.query(User).filter(func.lower(User.email) == func.lower(email))
        # Raise exception if we did not find exactly one user
        result = self.runUniqueQuery(query,"No users with that email", "Multiple users with that email")
        return result


    def addUserInfo(self,user,name,cgac_code,title):
        """ Called after registration, add all info to user.

        Arguments:
            user - User object
            name - Name of user
            cgac_code - CGAC Code of the agency of user
            title - Title of user
        """
        # Add info to user ORM
        user.name = name
        user.cgac_code = cgac_code
        user.title = title
        self.session.commit()

    def changeStatus(self,user,statusName):
        """ Change status for specified user

        Arguments:
            user - User object
            statusName - Status to change to
        """
        try:
            user.user_status_id = self.getUserStatusId(statusName)
        except ValueError as e:
            # In this case having a bad status name is a client error
            raise ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
        self.session.commit()

    def checkStatus(self,user,statusName):
        """ Check if a user has a specific status

        Arguments:
            user - User object
            statusName - Status to check against
        Returns:
            True if user has that status, False otherwise, raises an exception if status name is not valid
        """
        try:
            if(user.user_status_id == self.getUserStatusId(statusName) ):
                return True
            else :
                return False
        except ValueError as e:
            # In this case having a bad status name is a client error
            raise ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)

    def addUnconfirmedEmail(self,email):
        """ Create user with specified email

        Arguments:
            email - Add user with specified email
        """
        user = User(email = email)
        self.changeStatus(user,"awaiting_confirmation")
        self.setPermission(user,0) # Users start with no permissions
        self.session.add(user)
        self.session.commit()

    def getEmailTemplate(self,emailType):
        """ Get template for specified email type

        Arguments:
            emailType - Name of template to get
        Returns:
            EmailTemplate object
        """
        type_query = self.session.query(EmailTemplateType.email_template_type_id).filter(EmailTemplateType.name == emailType)
        type_result = self.runUniqueQuery(type_query, "No email template type with that name", "Multiple email templates type with that name")

        template_query = self.session.query(EmailTemplate).filter(EmailTemplate.template_type_id == type_result.email_template_type_id)
        template_result = self.runUniqueQuery(template_query, "No email template with that template type",
                                              "Multiple email templates with that template type")
        return template_result

    def getUsersByStatus(self,status,cgac_code=None):
        """ Return list of all users with specified status

        Arguments:
            status - Status to check against
        Returns:
            list of User objects
        """
        statusId = self.getUserStatusId(status)
        query = self.session.query(User).filter(User.user_status_id == statusId)
        if cgac_code is not None:
            query = query.filter(User.cgac_code == cgac_code)
        return query.all()

    def getStatusOfUser(self,user):
        """ Given a user object return their status as a string

        Arguments:
            user - User object
        Returns:
            status name (string)
        """
        return user.status.name

    def getStatusIdOfUser(self,user):
        """ Given a user object return status ID

        Arguments:
            user - User object
        Returns:
            status ID (int)
        """
        return user.status_id

    def getUsersByType(self,permissionName):
        """deprecated: moved to function_bag.py"""
        userList = []
        bitNumber = self.getPermissionId(permissionName)
        users = self.session.query(User).all()
        for user in users:
            if checkPermissionByBitNumber(user, bitNumber):
                # This user has this permission, include them in list
                userList.append(user)
        return userList

    def getUserPermissions(self, user):
        """ Get name for specified permissions for this user

        Arguments:
            user
        Returns:
            array of permission names
        """
        all_permissions = self.getPermissionList()
        user_permissions = []
        for permission in all_permissions:
            if self.hasPermission(user, permission.name):
                user_permissions.append(str(permission.name))
        return sorted(user_permissions, key=str.lower)

    def hasPermission(self, user, permissionName):
        """ Checks if user has specified permission

        Arguments:
            user - User object
            permissionName - permission to check
        Returns:
            True if user has the specified permission, False otherwise
        """
        # Get the bit number corresponding to this permission from the permission_types table
        bitNumber = self.getPermissionId(permissionName)
        # Use that bit number to check whether user has the specified permission
        if checkPermissionByBitNumber(user, bitNumber):
            return True
        return False

    def setPermission(self,user,permission):
        """ Define a user's permission to set value (overwrites all current permissions)

        Arguments:
            user - User object
            permission - new value for user's permissions
        """
        user.permissions = permission
        self.session.commit()

    def grantPermission(self,user,permissionName):
        """ Grant a user a permission specified by name, does not affect other permissions

        Arguments:
            user - User object
            permissionName - permission to grant
        """
        if(user.permissions == None):
            # Start users with zero permissions
            user.permissions = 0
        bitNumber = self.getPermissionId(permissionName)
        if not checkPermissionByBitNumber(user, bitNumber):
            # User does not have permission, grant it
            user.permissions = user.permissions + (2 ** bitNumber)
            self.session.commit()

    def removePermission(self,user,permissionName):
        """ Remove a permission specified by name from user

        Arguments:
            user - User object
            permissionName - permission to remove
        """
        if(user.permissions == None):
            # Start users with zero permissions
            user.permissions = 0
        bitNumber = self.getPermissionId(permissionName)
        if checkPermissionByBitNumber(user, bitNumber):
            # User has permission, remove it
            user.permissions = user.permissions - (2 ** bitNumber)
            self.session.commit()

    def getPermissionId(self,permissionName):
        """ Get ID for specified permission name

        Arguments:
            permissionName - permission to get ID for
        Returns:
            ID of this permission
        """
        query = self.session.query(PermissionType).filter(PermissionType.name == permissionName)
        result = self.runUniqueQuery(query,"Not a valid user type","Multiple permission entries for that type")
        return result.permission_type_id

    def checkPassword(self,user,password,bcrypt):
        """ Given a user object and a password, verify that the password is correct.

        Arguments:
            user - User object
            password - Password to check
            bcrypt - bcrypt to use for password hashing
        Returns:
             True if valid password, False otherwise.
        """
        if(password == None or password.strip()==""):
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

    def clearPassword(self,user):
        """ Clear a user's password as part of reset process

        Arguments:
            user - User object

        """
        user.salt = None
        user.password_hash = None
        self.session.commit()

    def getPermissionList(self):
        """ Gets the permission list

        Returns:
            list of PermissionType objects
        """
        queryResult = self.session.query(PermissionType).all()
        return queryResult

    def updateLastLogin(self, user, unlock_user=False):
        """ This updates the last login date to today's datetime for the user to the current date upon successful login.
        """
        user.last_login_date = time.strftime("%c") if not unlock_user else None
        self.session.commit()

    def setUserActive(self, user, is_active):
        """ Sets the is_active field for the specified user """
        user.is_active = is_active
        self.session.commit()
