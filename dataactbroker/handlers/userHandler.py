import uuid
import time

from dataactcore.models.userModel import User, PermissionType
from dataactcore.models.userInterface import UserInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.models.userModel import EmailToken, EmailTemplateType, EmailTemplate

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
            status_id = USER_STATUS_DICT[status]
            query = query.filter(User.user_status_id == status_id)
        if only_active:
            query = query.filter(User.is_active == True)
        return query.all()

    def deleteUser(self, email):
        """ Delete user with specified email.  Submissions from that user are not deleted, instead they have
        user id set to null  """

        # Delete user
        self.session.query(User).filter(User.email == email).delete()
        self.session.commit()

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

    def changeStatus(self,user,status_name):
        """ Change status for specified user

        Arguments:
            user - User object
            statusName - Status to change to
        """
        try:
            user.user_status_id = USER_STATUS_DICT[status_name]
        except ValueError as e:
            # In this case having a bad status name is a client error
            raise ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
        self.session.commit()

    def checkStatus(self,user,status_name):
        """ Check if a user has a specific status

        Arguments:
            user - User object
            statusName - Status to check against
        Returns:
            True if user has that status, False otherwise, raises an exception if status name is not valid
        """
        try:
            if user.user_status_id == USER_STATUS_DICT[status_name]:
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

    def clearPassword(self,user):
        """ Clear a user's password as part of reset process

        Arguments:
            user - User object

        """
        user.salt = None
        user.password_hash = None
        self.session.commit()

    def updateLastLogin(self, user, unlock_user=False):
        """ This updates the last login date to today's datetime for the user to the current date upon successful login.
        """
        user.last_login_date = time.strftime("%c") if not unlock_user else None
        self.session.commit()

    def setUserActive(self, user, is_active):
        """ Sets the is_active field for the specified user """
        user.is_active = is_active
        self.session.commit()
