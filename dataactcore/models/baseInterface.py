import sqlalchemy
from flask import _app_ctx_stack
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


class BaseInterface(object):
    """ Abstract base interface to be inherited by interfaces for specific databases
    """
    #For Flask Apps use the context for locals
    IS_FLASK = True
    dbName = None  # Should be overwritten by child classes
    dbConfig = None  # Should be overwritten by child classes
    logFileName = "dbErrors.log"

    def __init__(self):
        if(self.session != None):
            # session is already set up for this DB
            return

        if not self.dbName:
            # Child class needs to set these before calling base constructor
            raise ValueError("Need dbName defined")

        if not self.dbConfig:
            raise ValueError("Database configuration is not defined")

        # Create sqlalchemy connection and session
        self.engine = sqlalchemy.create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(self.dbConfig["username"],
            self.dbConfig["password"], self.dbConfig["host"], self.dbConfig["port"],
            self.dbName), pool_size=100,max_overflow=50)
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
            'username': self.dbConfig['username'],
            'password': self.dbConfig['password'],
            'host': self.dbConfig['host'],
            'port': self.dbConfig['port'],
            'dbBaseName': self.dbConfig['base_db_name']
        }
        return credDict

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

    def getIdFromDict(self, model, dictName, fieldName, fieldValue, idField):
        """ Populate a static dictionary to hold an id to name dictionary for specified model """
        dict = getattr(model, dictName)
        if(dict == None):
            dict = {}
            # Pull status values out of DB
            # Create new session for this
            queryResult = self.session.query(model).all()
            for result in queryResult:
                dict[getattr(result,fieldName)] = getattr(result,idField)
            setattr(model,dictName,dict)
        if fieldValue is None:
            # Not looking for a return, just called to set up dict
            return None
        if(not fieldValue in dict):
            raise ValueError("Not a valid " + str(model) + ": " + str(fieldValue) + ", not found in dict: " + str(dict))
        return dict[fieldValue]

    def getNameFromDict(self, model, dictName, fieldName, fieldValue, idField):
        """ This uses the dict attached to model backwards, to get the name from the ID.  This is slow and should not
        be used too widely """
        # Populate dict
        self.getIdFromDict(model, dictName, fieldName, None, idField)
        # Step through dict to find fieldValue
        dict = model.__dict__[dictName]
        for key in dict:
            if dict[key] == fieldValue:
                return key
        # If not found, raise an exception
        raise ValueError("Value: " + str(fieldValue) + " not found in dict: " + str(dict))
