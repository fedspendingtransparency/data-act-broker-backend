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
