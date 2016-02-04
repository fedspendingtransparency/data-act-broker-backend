from dataactcore.models.userModel import User
from sqlalchemy.orm.exc import MultipleResultsFound
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
