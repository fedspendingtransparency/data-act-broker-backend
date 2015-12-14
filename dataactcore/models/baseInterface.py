import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
import os
import inspect
from dataactcore.utils.responseException import ResponseException
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound

class BaseInterface(object):
    """ Abstract base interface to be inherited by interfaces for specific databases
    """

    dbConfigFile = None # Should be overwritten by child classes
    dbName = None # Should be overwritten by child classes
    credFileName = None

    def __init__(self):
        if(self.dbConfigFile == None or self.dbName == None):
            # Child class needs to set these before calling base constructor
            raise ValueError("Need dbConfigFile and dbName defined")
        # Load config info
        try:
            confDict = json.loads(open(self.dbConfigFile,"r").read())
        except IOError:
            raise IOError(str(self.dbConfigFile))
        # Create sqlalchemy connection and session
        self.engine = sqlalchemy.create_engine("postgresql://" + confDict["username"] + ":" + confDict["password"] + "@" + confDict["host"] + ":" + confDict["port"] + "/" + self.dbName)
        self.connection = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    @classmethod
    def getCredDict(cls):
        """ Gets credentials dictionary """
        return json.loads(open(cls.getCredFilePath(),"r").read())

    @classmethod
    def getCredFilePath(cls):
        """  Returns full path to credentials file """
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        lastBackSlash = path.rfind("\\",0,-1)
        lastForwardSlash = path.rfind("/",0,-1)
        lastSlash = max([lastBackSlash,lastForwardSlash])
        return path[0:lastSlash] + "/credentials/" + cls.credFileName

    @staticmethod
    def checkUnique(queryResult, noResultMessage, multipleResultMessage):
        """ Check that result is unique, if not raise exception"""
        if(len(queryResult) == 0):
                # Did not get a result for this job
            exc = ResponseException(noResultMessage)
            exc.status = 400
            exc.wrappedException = NoResultFound(noResultMessage)
            raise exc
        elif(len(queryResult) > 1):
            # Multiple results for single job ID
            exc = ResponseException(multipleResultMessage)
            exc.status = 400
            exc.wrappedException = MultipleResultsFound(multipleResultMessage)
            raise exc
        return True

    def runStatement(self,statement):
        """ Run specified statement on this database"""
        self.connection.execute(statement)
        return True
