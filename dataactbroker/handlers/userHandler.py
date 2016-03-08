import uuid
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from dataactcore.models.userModel import User, PermissionType
from dataactcore.models.userInterface import UserInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.models.brokerUserModels import EmailToken, EmailTemplateType , EmailTemplate

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

    def saveToken(self,salt,token):
        """ saves token into database

        Arguments:
            token - Token to save in the database
            salt - Salt to save for this token
        Returns:
            No return value
        """
        newToken = EmailToken()
        newToken.salt = salt
        newToken.token = token
        self.session.add(newToken)
        self.session.commit()

    def deleteToken(self,token):
        """ deletes old token

        Arguments:
            token - Token to be deleted
        """
        oldToken = self.session.query(EmailToken).filter(EmailToken.token == token).one()
        self.session.delete(oldToken)
        self.session.commit()

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


    def addUserInfo(self,user,name,agency,title):
        """ Called after registration, add all info to user.

        Arguments:
            user - User object
            name - Name of user
            agency - Agency of user
            title - Title of user
        """
        # Add info to user ORM
        user.name = name
        user.agency = agency
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
        emailId = self.session.query(EmailTemplateType.email_template_type_id).filter(EmailTemplateType.name == emailType).one()
        return self.session.query(EmailTemplate).filter(EmailTemplate.template_type_id == emailId).one()

    def getUsersByStatus(self,status):
        """ Return list of all users with specified status

        Arguments:
            status - Status to check against
        Returns:
            list of User objects
        """
        statusId = self.getUserStatusId(status)
        return self.session.query(User).filter(User.user_status_id == statusId).all()

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
        """ Get all users that have the specified permission

        Arguments:
            permissionName - permission to check against
        Returns:
            list of all users that have that permission
        """
        userList = []
        bitNumber = self.getPermissionId(permissionName)
        users = self.session.query(User).all()
        for user in users:
            if self.checkPermissionByBitNumber(user, bitNumber):
                # This user has this permission, include them in list
                userList.append(user)
        return userList


    def hasPermisson(self,user,permissionName):
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
        if self.checkPermissionByBitNumber(user, bitNumber):
            return True
        return False


    @staticmethod
    def checkPermissionByBitNumber(user, bitNumber):
        """ Check whether user has the specified permission, determined by whether a binary representation of user's
        permissions has the specified bit set to 1.  Use hasPermission to check by permission name.

        Arguments:
            user - User object
            bitNumber - int representing position of bit that corresponds to permission to check (0 checks least significant bit)
        Returns:
            True if user has that permission, False otherwise
        """
        if(user.permissions == None):
            # This user has no permissions
            return False
        # First get the value corresponding to the specified bit (i.e. 2^bitNumber)
        bitValue = 2 ** (bitNumber)
        # Remove all bits above the target bit by modding with the value of the next higher bit
        # This leaves the target bit and all lower bits as the remaining value, all higher bits are set to 0
        lowEnd = user.permissions % (bitValue * 2)
        # Now compare the remaining value to the value for the target bit to determine if that bit is 0 or 1
        # If the remaining value is still at least the value of the target bit, that bit is 1, so we have that permission
        return (lowEnd >= bitValue)

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
        if not self.checkPermissionByBitNumber(user, bitNumber):
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
        if self.checkPermissionByBitNumber(user, bitNumber):
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
        user.password_hash = bcrypt.generate_password_hash(password+newSalt,UserHandler.HASH_ROUNDS)
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

    def getPermssionList(self):
        """ Gets the permission list

        Returns:
            list of PermissionType objects
        """
        queryResult = self.session.query(PermissionType).all()
        return queryResult

    def createUserWithPassword(self,email,password,bcrypt,admin=False):
        """ This directly creates a valid user in the database with password and permissions set.

        Arguments:
            email - Email for new user
            password - Password to assign to user
            bcrypt - bcrypt to use for password hashing
            admin - Whether the new user should be an admin
        """
        user = User(email = email)
        self.session.add(user)
        self.setPassword(user,password,bcrypt)
        self.changeStatus(user,"approved")
        if(admin):
            self.setPermission(user,2)
        else:
            self.setPermission(user,1)
        self.session.commit()

    def loadEmailTemplate(self,subject,contents,emailType):
        emailId = self.session.query(EmailTemplateType.email_template_type_id).filter(EmailTemplateType.name == emailType).one()
        template = EmailTemplate()
        template.subject = subject
        template.content = contents
        template.template_type_id = emailId
        self.session.add(template)
        self.session.commit()