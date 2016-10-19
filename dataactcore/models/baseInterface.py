from collections import namedtuple
import logging

import sqlalchemy
import flask
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.config import CONFIG_DB


logger = logging.getLogger(__name__)


class _DB(namedtuple('_DB', ['engine', 'connection', 'Session', 'session'])):
    """Represents a database connection, from engine to session"""
    def close(self):
        self.session.close()
        self.Session.remove()
        self.connection.close()
        self.engine.dispose()


class GlobalDB:
    @classmethod
    def _holder(cls):
        """We generally want to work in the `g` context (i.e. per request),
        but there are paths through the app which won't have access. In those
        situations, fall back to the non-threadsafe static member approach"""
        if flask.current_app:
            return flask.g
        else:
            logger.warning("No current_app, falling back to non-threadsafe "
                           "database connection")
            return cls

    @classmethod
    def db(cls):
        """Build or retrieve the database information"""
        holder = cls._holder()
        if not getattr(holder, '_db', None):
            holder._db = dbConnection()
        return holder._db

    @classmethod
    def close(cls):
        """Close the database connection, if present"""
        holder = cls._holder()
        if hasattr(holder, '_db'):
            holder._db.close()
            del holder._db


class BaseInterface(object):
    """ Abstract base interface to be inherited by interfaces for specific databases
    """
    dbConfig = None
    logFileName = "dbErrors.log"
    dbName = None
    # This holds a pointer to an InterfaceHolder object, and is populated when that is instantiated
    interfaces = None

    def __init__(self):
        self.dbConfig = CONFIG_DB
        self.dbName = self.dbConfig['db_name']

    @property
    def engine(self):
        return GlobalDB.db().engine

    @property
    def connection(self):
        return GlobalDB.db().connection

    @property
    def session(self):
        return GlobalDB.db().session

    def __del__(self):
        self.close()

    def close(self):
        self.interfaces = None
        GlobalDB.close()

    @classmethod
    def getCredDict(cls):
        """ Return db credentials. """
        credDict = {
            'username': cls.dbConfig['username'],
            'password': cls.dbConfig['password'],
            'host': cls.dbConfig['host'],
            'port': cls.dbConfig['port'],
            'dbBaseName': cls.dbConfig['base_db_name'],
            'scheme': cls.dbConfig['scheme']
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
        """ Populate a static dictionary to hold an id to name dictionary for specified model

        Args:
            model - Model to populate dictionary for
            dictName - Name of dictionary to be populated
            fieldName - Field that will be used to populate keys of dictionary
            fieldValue - Value being queried for (None to just set up dict without returning)
            idField - Field that will be used to populate values of dictionary

        Returns:
            Value in idField that corresponds to specified fieldValue in fieldName
        """
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


def dbConnection():
    """Use the config to set up a database engine and connection"""
    if not CONFIG_DB:
        raise ValueError("Database configuration is not defined")

    dbName = CONFIG_DB['db_name']
    if not dbName:
        raise ValueError("Need dbName defined")

    # Create sqlalchemy connection and session
    engine = sqlalchemy.create_engine(
        "postgresql://{username}:{password}@{host}:{port}/{db_name}".format(
            **CONFIG_DB),
        pool_size=100, max_overflow=50)
    connection = engine.connect()
    Session = scoped_session(sessionmaker(bind=engine, autoflush=True))
    return _DB(engine, connection, Session, Session())
