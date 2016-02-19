from sqlalchemy.orm.exc import MultipleResultsFound
from dataactcore.models.userModel import User, UserStatus, EmailToken
from dataactcore.models.userInterface import UserInterface

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

    def getTokenSalt(self,token):
        """gets the salt from a given token so it can be decoded"""
        return  self.session.query(EmailToken.salt).filter(EmailToken.token == token).one()

    def saveToken(self,salt,token):
        newToken = EmailToken()
        newToken.salt = salt
        newToken.token = token
        self.session.add(newToken)
        self.session.commit()


    def getUserId(self, username):
        """ Find an id for specified username, creates a new entry for new usernames, raises an exception if multiple results found

        Arguments:
        username - username to find an id for
        Returns:
        user_id to be used by session handler
        """
        # Check if user exists
        queryResult = self.session.query(User.user_id).filter(User.username == username).all()
        if(len(queryResult) == 1):
            # If so, return ID
            return queryResult[0].user_id
        elif(len(queryResult) == 0):
            # If not, add new user
            newUser = User(username = username)
            self.session.add(newUser)
            self.session.commit()
            return newUser.user_id
        else:
            # Multiple entries for this user, server error
            raise MultipleResultsFound("Multiple entries for single username")

    def getUserByEmail(self,email):
        """ Return a User object that matches specified email """
        result = self.session.query(User).filter(User.email == email).all()
        # Raise exception if we did not find exactly one user
        self.checkUnique(result,"No users with that email", "Multiple users with that email")
        return result[0]

    def addUserInfo(self,user,name,agency,title):
        """ Called after registration, add all info to user. """
        # Add info to user ORM
        user.name = name
        user.agency = agency
        user.title = title
        self.session.commit()

    def changeStatus(self,user,statusName):
        """ Change status for specified user """
        user.user_status_id = UserStatus.getStatus(statusName)
        self.session.commit()

    def addUnconfirmedEmail(self,email):
        """ Create user with specified email """
        # Check for presence of email in database, raise exception if present
        result = self.session.query(User).filter(User.email == email).all()
        if(len(result) > 0):
            # Email already in use
            raise ValueError("That email is already associated with a user")
        user = User(email = email)
        self.changeStatus(user,"awaiting_confirmation")
        self.session.add(user)
        self.session.commit()
