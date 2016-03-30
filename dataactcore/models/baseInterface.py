import sqlalchemy
import os.path
import sys
import traceback
from flask import _app_ctx_stack
from sqlalchemy.orm import sessionmaker , scoped_session
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.config import CONFIG_DB, CONFIG_LOGGING

class BaseInterface(object):
    """ Abstract base interface to be inherited by interfaces for specific databases
    """
    #For Flask Apps use the context for locals
    IS_FLASK = True
    dbName = None # Should be overwritten by child classes
    logFileName = "dbErrors.log"

    def __init__(self):
        if(self.session != None):
            # session is already set up for this DB
            return

        if not self.dbName:
            # Child class needs to set these before calling base constructor
            raise ValueError("Need dbName defined")

        # Create sqlalchemy connection and session
        self.engine = sqlalchemy.create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(CONFIG_DB["username"],
            CONFIG_DB["password"], CONFIG_DB["host"], CONFIG_DB["port"],
            self.dbName), pool_size=100,max_overflow=50)
            #"postgresql://" + CONFIG_DB['username'] + ":" + CONFIG_DB['password'] + "@" + CONFIG_DB['host'] + ":" + CONFIG_DB['port'] + "/" + self.dbName,pool_size=100,max_overflow=50)
        self.connection = self.engine.connect()
        if(self.Session == None):
            if(BaseInterface.IS_FLASK) :
                self.Session = scoped_session(sessionmaker(bind=self.engine,autoflush=True),scopefunc=_app_ctx_stack.__ident_func__)
            else :
                self.Session = scoped_session(sessionmaker(bind=self.engine,autoflush=True))
        self.session = self.Session()

    def __del__(self):
        try:
            #Close session
            self.session.close()
            #self.Session.close_all()
            self.Session.remove()
            self.connection.close()
            self.engine.dispose()
        except KeyError:
            # KeyError will occur in Python 3 on engine dispose
            pass

    @classmethod
    def getCredDict(cls):
        """ Return db credentials. """
        credDict = {
            'username': CONFIG_DB['username'],
            'password': CONFIG_DB['password'],
            'host': CONFIG_DB['host'],
            'port': CONFIG_DB['port'],
            'dbBaseName': CONFIG_DB['base_db_name']
        }
        return credDict

    @staticmethod
    def logDbError(exc):
        logFile = os.path.join(
            CONFIG_LOGGING['log_files'], BaseInterface.logFileName)
        with open(logFile, "a") as file:
        #file = open(BaseInterface.getLogFilePath(),"a")
            file.write(str(exc) + ", ")
            file.write(str(sys.exc_info()[0:1]) + "\n")
            traceback.print_tb(sys.exc_info()[2], file=file)

    @staticmethod
    def checkUnique(queryResult, noResultMessage, multipleResultMessage):
        """ Check that result is unique, if not raise exception"""
        if(len(queryResult) == 0):
            # Did not get a result for this job, mark as a job error
            raise ResponseException(noResultMessage,StatusCode.CLIENT_ERROR,NoResultFound)

        elif(len(queryResult) > 1):
            # Multiple results for single job ID
            raise ResponseException(multipleResultMessage,StatusCode.INTERNAL_ERROR,MultipleResultsFound)

        return True

    @staticmethod
    def runUniqueQuery(query, noResultMessage, multipleResultMessage):
        """ Run query looking for one result, if it fails wrap it in a ResponseException with an appropriate message """
        try:
            return query.one()
        except NoResultFound as e:
            if(noResultMessage == False):
                # Raise the exception as is, used for specific handling
                raise e
            raise ResponseException(noResultMessage,StatusCode.CLIENT_ERROR,NoResultFound)
        except MultipleResultsFound as e:
            raise ResponseException(multipleResultMessage,StatusCode.INTERNAL_ERROR,MultipleResultsFound)

    def runStatement(self,statement):
        """ Run specified statement on this database"""
        response =  self.session.execute(statement)
        self.session.commit()
        return response

    def getIdFromDict(self,model, dictName, fieldName, fieldValue, idField):
        dict = getattr(model, dictName)
        if(dict == None):
            dict = {}
            # Pull status values out of DB
            # Create new session for this
            queryResult = self.session.query(model).all()
            for result in queryResult:
                dict[getattr(result,fieldName)] = getattr(result,idField)
            setattr(model,dictName,dict)
        if(not fieldValue in dict):
            raise ValueError("Not a valid " + str(model) + ": " + str(fieldValue) + ", not found in dict: " + str(dict))
        return dict[fieldValue]