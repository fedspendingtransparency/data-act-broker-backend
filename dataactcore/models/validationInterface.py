import sqlalchemy
import json
from dataactcore.models.baseInterface import BaseInterface
import os
import inspect
from dataactcore.utils.responseException import ResponseException
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
from validationModels import Base
class ValidationInterface(BaseInterface):
    """ Manages all interaction with the validation database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """

    dbName = "validation"
    credFileName = "dbCred.json"

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(ValidationInterface,self).__init__()
        Base.metadata.bind = self.engine
        Base.metadata.create_all(self.engine)


    @staticmethod
    def getDbName():
        """ Return database name"""
        return ValidationInterface.dbName
