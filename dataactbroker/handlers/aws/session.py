from json import loads, dumps
from uuid import uuid4
from datetime import datetime, timedelta
from flask.sessions import SessionInterface, SessionMixin
from flask_login import _create_identifier
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import SessionMap


class LoginSession:
    """
    This class is a wrapper for the session object
    """
    @staticmethod
    def logout(session):
        """
        arguments:

        session -- (Session) the session object

        Clears the current session
        """
        session.pop("login", None)
        session.pop("name", None)

    @staticmethod
    def login(session, username):
        """
        arguments:

        session -- (Session) the session object

        username -- (int) the id of the user

        Sets the current session status
        """
        session["name"] = username
        session["login"] = True

    @staticmethod
    def resetID(session):
        """
        arguments:

        session -- (Session) the session object

        resets the _uid in cases that the session becomes invalid
        """
        session["_uid"] = "{}|{}".format(_create_identifier(), uuid4())


def toUnixTime(datetimeValue):
    """
    arguments:

    datetimeValue -- (DateTime)

    Converts datetimeValue to time in seconds ince 1970

    returns int
    """
    if isinstance(datetimeValue, datetime):
        # If argument is a datetime object, convert to timestamp
        return (datetimeValue - datetime(1970, 1, 1)).total_seconds()
    return datetimeValue

class UserSession(dict, SessionMixin):
    """
    Class that wraps around normal Flask Session object
    """
    pass

class UserSessionInterface(SessionInterface):
    """

    Class That implements the SessionInterface and uses SessionTable to store data

    """

    SESSSION_CLEAR_COUNT_LIMIT = 10

    CountLimit = 1

    def __init__(self):
        return

    def open_session(self, app, request):
        """

        arguments:

        app -- (Flask) the Flask applcation
        request -- (Request)  the request object

        implements the open_session method that pulls or creates a new UserSession object

        """
        sid = request.headers.get("x-session-id")
        if sid and SessionTable.doesSessionExist(sid):
            if SessionTable.getTimeout(sid) > toUnixTime(datetime.utcnow()):
                session_dict = UserSession()
                # Read data as json
                data = loads(SessionTable.getData(sid))
                for key in data.keys():
                    session_dict[key] = data[key]
                return session_dict
        # This can be made better most likely need to do research
        # Maybe Hash(time + server id + random number)? Want to prevent any conflicts
        sid = str(uuid4())
        session_dict = UserSession()
        session_dict["sid"] = sid
        return session_dict

    def save_session(self, app, session, response):
        """
        arguments:

        app -- (Flask) the Flask applcation
        request -- (Request)  the request object
        session -- (Session)  the session object

        implements the save_session method that saves the session or clears it
        based on the timeout limit, this function also extends the expiration time of the current session

        """
        if not session:
            return
        # Extend the expiration based on either the time out limit set here or the permanent_session_lifetime property of the app
        if self.get_expiration_time(app, session):
            expiration = self.get_expiration_time(app, session)
        else:
            if "session_check" in session and session["session_check"] and SessionTable.doesSessionExist(session["sid"]):
                # This is just a session check, don't extend expiration time
                expiration = SessionTable.getTimeout(session["sid"])
                # Make sure next route call does not get counted as session check
                session["session_check"] = False
            else:
                expiration = datetime.utcnow() + timedelta(seconds=SessionTable.TIME_OUT_LIMIT)
        if "_uid" not in session:
            LoginSession.resetID(session)
        SessionTable.newSession(session["sid"], session, expiration)
        UserSessionInterface.CountLimit += 1
        if UserSessionInterface.CountLimit % UserSessionInterface.SESSSION_CLEAR_COUNT_LIMIT == 0:
            SessionTable.clearSessions()
            UserSessionInterface.CountLimit = 1

        # Return session ID as header x-session-id
        response.headers["x-session-id"] = session["sid"]

class SessionTable:
    """
    Provides helper functions for session management

    Constants :

    TIME_OUT_LIMIT -- (int) The limit used for the session

    """
    TIME_OUT_LIMIT = 604800

    @staticmethod
    def clearSessions():
        """
        Removes old sessions that are expired
        """
        newTime = toUnixTime(datetime.utcnow())
        sess = GlobalDB.db().session
        sess.query(SessionMap).filter(SessionMap.expiration < newTime).delete()
        sess.commit()

    @staticmethod
    def doesSessionExist(uid):
        """
        arguments:

        uid -- (String) the uid
        return (boolean) if the session
        """
        item = GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one_or_none()
        if item is not None:
            # session found
            return True
        else:
            return False

    @staticmethod
    def getTimeout(uid):
        """
        arguments:

        uid -- (String) the uid
        return (int) time when the session expires
        """
        return GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one().expiration

    @staticmethod
    def getData(uid):
        """
        uid -- (String) the uid
        return (Session) the session data
        """
        return GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one().data

    @staticmethod
    def newSession(uid, data, expiration):
        """ Updates current session or creates a new one if no session exists
        arguments:

        uid  -- (String) the session id
        data -- (String) the data for the session
        expiration -- (int) the time in seconds from 1970 when the session is no longer active

        Updates the exsiting session or creates a new one
        """
        # Try converting session to json
        sess = GlobalDB.db().session
        user_session = sess.query(SessionMap).filter_by(uid=uid).one_or_none()
        if user_session is None:
            # No existing session found, create a new one
            new_session = SessionMap(uid=uid, data=dumps(data), expiration=toUnixTime(expiration))
            sess.add(new_session)
        else:
            # Modify existing session
            user_session.data = dumps(data)
            user_session.expiration = toUnixTime(expiration)
        sess.commit()
