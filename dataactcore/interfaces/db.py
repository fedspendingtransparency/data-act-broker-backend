from collections import namedtuple
from contextlib import contextmanager
import logging
import sqlalchemy
import flask
from sqlalchemy.orm import sessionmaker, scoped_session
from dataactcore.config import CONFIG_DB


logger = logging.getLogger(__name__)


class _DB(namedtuple('_DB', ['engine', 'connection', 'Session', 'session'])):
    """Represents a database connection, from engine to session."""
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


def dbConnection():
    """Use the config to set up a database engine and connection."""
    if not CONFIG_DB:
        raise ValueError("Database configuration is not defined")

    dbName = CONFIG_DB['db_name']
    if not dbName:
        raise ValueError("Need dbName defined")

    # Create sqlalchemy connection and session
    uri = dbURI(dbName)
    engine = sqlalchemy.create_engine(uri, pool_size=100, max_overflow=50)
    connection = engine.connect()
    Session = scoped_session(sessionmaker(bind=engine))
    return _DB(engine, connection, Session, Session())


def dbURI(dbName):
    uri = "postgresql://{username}:{password}@{host}:{port}/{}".format(
        dbName, **CONFIG_DB)
    return uri


@contextmanager
def databaseSession():
    # option for application components
    # that need to access the database
    # outside of a Flask request
    # context
    db = dbConnection()
    yield db.session
    db.close()
