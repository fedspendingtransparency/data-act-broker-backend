import sqlalchemy
import json
from dataactcore.models.baseInterface import BaseInterface
import os
import inspect
from dataactcore.utils.responseException import ResponseException
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
class UserInterface(BaseInterface):
    """ Manages all interaction with the validation database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """

    dbName = "user_manager"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(UserInterface,self).__init__()


    @staticmethod
    def getDbName():
        """ Return database name"""
        return UserInterface.dbName
