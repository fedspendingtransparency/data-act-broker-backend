from json import loads, dumps
from uuid import uuid4
from datetime import datetime, timedelta
from flask.sessions import SessionInterface, SessionMixin
from flask_login import _create_identifier
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import SessionMap


class LoginSession:
    """ This class is a wrapper for the session object """
    @staticmethod
    def logout(session):
        """ Clears the current session

            Args:
                session: the Session object
        """
        session.pop("login", None)
        session.pop("name", None)

    @staticmethod
    def login(session, username):
        """ Sets the current session status

            Args:
                session: the Session object
                username: the id of the user
        """
        session["name"] = username
        session["login"] = True

    @staticmethod
    def reset_id(session):
        """ Resets the _uid in cases that the session becomes invalid

            Args:
                session: the Session object
        """
        session["_uid"] = "{}|{}".format(_create_identifier(), uuid4())


def to_unix_time(datetime_value):
    """ Converts datetime_value to time in seconds since 1970

        Args:
            datetime_value: datetime value to convert

        Returns:
            Integer value that represents the given datetime value in seconds since 1970 if the value provided is valid,
            else returns the initial value provided without changes
    """
    # If argument is a datetime object, convert to timestamp
    if isinstance(datetime_value, datetime):
        return (datetime_value - datetime(1970, 1, 1)).total_seconds()
    return datetime_value


class UserSession(dict, SessionMixin):
    """ Class that wraps around normal Flask Session object """
    pass


class UserSessionInterface(SessionInterface):
    """ Class That implements the SessionInterface and uses SessionTable to store data """

    def __init__(self):
        """ Initializes the UserSessionInterface """
        return

    def open_session(self, app, request):
        """ Pulls or creates a new UserSession object

            Args:
                app: the Flask application
                request: the Request object
        """
        sid = request.headers.get("x-session-id")
        if sid and SessionTable.does_session_exist(sid):
            if SessionTable.get_timeout(sid) > to_unix_time(datetime.utcnow()):
                session_dict = UserSession()
                # Read data as json
                data = loads(SessionTable.get_data(sid))
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
        """ Saves the session or clears it based on the timeout limit. Also extends the expiration time of the current
            session

            Args:
                app: the Flask application
                request: the Request object
                session: the Session object
        """
        if not session:
            return
        # Extend the expiration based on either the time out limit set here or the permanent_session_lifetime property
        # of the app
        if self.get_expiration_time(app, session):
            expiration = self.get_expiration_time(app, session)
        else:
            if "session_check" in session and session["session_check"] and \
                    SessionTable.does_session_exist(session["sid"]):
                # This is just a session check, don't extend expiration time
                expiration = SessionTable.get_timeout(session["sid"])
                # Make sure next route call does not get counted as session check
                session["session_check"] = False
            else:
                expiration = datetime.utcnow() + timedelta(seconds=SessionTable.TIME_OUT_LIMIT)
        if "_uid" not in session:
            LoginSession.reset_id(session)
        SessionTable.new_session(session["sid"], session, expiration)
        SessionTable.clear_sessions()

        # Return session ID as header x-session-id
        response.headers["x-session-id"] = session["sid"]


class SessionTable:
    """ Provides helper functions for session management

        Constants :
            TIME_OUT_LIMIT: The limit (in seconds) for the session before it times out
    """
    TIME_OUT_LIMIT = 1800

    @staticmethod
    def clear_sessions():
        """ Removes old sessions that are expired """
        new_time = to_unix_time(datetime.utcnow())
        sess = GlobalDB.db().session
        sess.query(SessionMap).filter(SessionMap.expiration < new_time).delete()
        sess.commit()

    @staticmethod
    def does_session_exist(uid):
        """ Checks if a session exists for the given uid

            Args:
                uid: the id of the session

            Returns:
                A boolean that indicates if the session exists or not
        """
        item = GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one_or_none()
        if item is not None:
            # session found
            return True
        else:
            return False

    @staticmethod
    def get_timeout(uid):
        """ Gets the timeout for the session

            Args:
                uid: the id of the session

            Returns:
                An integer indicating the expiration date/time of the given session
        """
        return GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one().expiration

    @staticmethod
    def get_data(uid):
        """ Gets the data for the session

            Args:
                uid: the id of the session

            Returns:
                An object containing the data of the session, including _uid, login, sid, session_check, and name
        """
        return GlobalDB.db().session.query(SessionMap).filter_by(uid=uid).one().data

    @staticmethod
    def new_session(uid, data, expiration):
        """ Updates current session or creates a new one if no session exists

            Args:
                uid: the id of the session
                data: the data for the session
                expiration: the time in seconds from 1970 when the session is no longer active
        """
        # Try converting session to json
        sess = GlobalDB.db().session
        user_session = sess.query(SessionMap).filter_by(uid=uid).one_or_none()
        if user_session is None:
            # No existing session found, create a new one
            new_session = SessionMap(uid=uid, data=dumps(data), expiration=to_unix_time(expiration))
            sess.add(new_session)
        else:
            # Modify existing session
            user_session.data = dumps(data)
            user_session.expiration = to_unix_time(expiration)
        sess.commit()
