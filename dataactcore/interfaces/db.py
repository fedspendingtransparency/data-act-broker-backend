from collections import namedtuple
import logging
import sqlalchemy
import flask
from sqlalchemy.orm import sessionmaker, scoped_session
from dataactcore.config import CONFIG_DB


logger = logging.getLogger(__name__)


class _DB(namedtuple('_DB', ['engine', 'connection', 'scoped_session_maker', 'session'])):
    """Represents a database connection, from engine to session."""
    def close(self):
        self.session.close()
        self.scoped_session_maker.remove()
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
            logger.warning("No current_app, falling back to non-threadsafe database connection")
            return cls

    @classmethod
    def db(cls):
        """Build or retrieve the database information"""
        holder = cls._holder()
        if not getattr(holder, '_db', None):
            holder._db = db_connection()
        return holder._db

    @classmethod
    def close(cls):
        """Close the database connection, if present"""
        holder = cls._holder()
        if hasattr(holder, '_db'):
            holder._db.close()
            del holder._db


def db_connection():
    """Use the config to set up a database engine and connection."""
    if not CONFIG_DB:
        raise ValueError("Database configuration is not defined")

    db_name = CONFIG_DB['db_name']
    if not db_name:
        raise ValueError("Need dbName defined")

    # Create sqlalchemy connection and session
    uri = db_uri(db_name)
    engine = sqlalchemy.create_engine(uri, pool_size=100, max_overflow=50)
    connection = engine.connect()
    scoped_session_maker = scoped_session(sessionmaker(bind=engine))
    return _DB(engine, connection, scoped_session_maker, scoped_session_maker())


def db_uri(db_name):
    uri = "postgresql://{username}:{password}@{host}:{port}/{}".format(db_name, **CONFIG_DB)
    return uri
