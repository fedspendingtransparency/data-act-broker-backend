from uuid import uuid4
from decimal import Decimal
from boto.dynamodb2.fields import HashKey, GlobalAllIndex
from boto.dynamodb2.table import Table, exceptions
from boto.dynamodb2.types import NUMBER
from datetime import datetime, timedelta
from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.layer1 import DynamoDBConnection
from flask_login import _create_identifier
from dataactcore.utils.requestDictionary import RequestDictionary

class LoginSession():
    """
    This class is a wrapper for the session object
    """

    @staticmethod
    def getName(session) :

        """
        arguments:

        session -- (Session) the session object

        returns the name id (string)
        """
        if session.get('name') is not None :
            return session['name']
        return ""

    @staticmethod
    def isLogin(session) :
        """
        arguments:

        session -- (Session) the session object

        returns (boolean) the status of the user session
        """

        if session.get('login') is not None :
            return True
        return False

    @staticmethod
    def logout(session) :
        """
        arguments:

        session -- (Session) the session object

        Clears the current session
        """

        session.pop("login", None)
        session.pop("name", None)
        session.pop("register", None)
        session.pop("reset", None)

    @staticmethod
    def login(session,username) :
        """
        arguments:

        session -- (Session) the session object

        username -- (int) the id of the user

        Sets the current session status
        """
        session["name"] =  username
        session["login"] = True
        session.pop("register", None)
        session.pop("reset", None)

    @staticmethod
    def register(session):
        """
        arguments:

        session -- (Session) the session object


        Marks the session that it has a real email
        address so it finish registering

        """
        session["register"] = True


    @staticmethod
    def resetPassword(session):
        """
        arguments:

        session -- (Session) the session object

        Marks the session that it has a real email
        address so it finish reseting the password

        """
        session["reset"] = True

    @staticmethod
    def isRegistering(session) :
        """
        arguments:

        session -- (Session) the session object

        returns (boolean) the status of the user session
        """
        if session.get('register') is not None :
            return True
        return False

    @staticmethod
    def isResetingPassword(session) :
        """
        arguments:

        session -- (Session) the session object

        returns (boolean) the status of the user session
        """
        if session.get('reset') is not None :
            return True
        return False


    @staticmethod
    def resetID(session):
        """
        arguments:

        session -- (Session) the session object

        resets the _uid in cases that the session becomes invalid
        """
        session["_uid"] = _create_identifier()

    @staticmethod
    def isSessionSecure(session):
        """
        arguments:

        session -- (Session) the session object

        checks if the user is the one who created the session.

        """
        if( "_uid" in session):
            if(not session["_uid"] ==  _create_identifier()):
                return False
            return True
        else :
            return False

def toUnixTime(datetimeValue) :
    """
    arguments:

    datetimeValue -- (DateTime)

    Converts datetimeValue to time in seconds ince 1970

    returns int
    """
    if(type(datetimeValue) == Decimal):
        # If argument is already Decimal, assume it's been converted previously
        return datetimeValue
    return (datetimeValue-datetime(1970,1,1)).total_seconds()



class DynamoSession(CallbackDict, SessionMixin):
    """
    Class that wraps around normal Flask Session object

    arguments:
    initial -- (Session) session
    sid --   (String) the uuid string from a cookie

    """
    def __init__(self, initial=None, sid=None):
        CallbackDict.__init__(self, initial)
        self.sid = sid
        self.modified = False



class DynamoInterface(SessionInterface):
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

        implements the open_session method that pulls or creates a new DynamoSession object

        """
        sid = request.headers.get("x-session-id")
        if(sid and SessionTable.doesSessionExist(sid)):
            if SessionTable.getTimeout(sid)> toUnixTime(datetime.utcnow()):
                return DynamoSession(initial=SessionTable.getData(sid),sid=sid)
        # This can be made better most likely need to do research
        # Maybe Hash(time + server id + random number)? Want to prevent any conflicts
        sid = str(uuid4())

        return DynamoSession(sid=sid)


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
            if "session_check" in session and session["session_check"] and SessionTable.doesSessionExist(session.sid):
                # This is just a session check, don't extend expiration time
                expiration = SessionTable.getTimeout(session.sid)
                # Make sure next route call does not get counted as session check
                session["session_check"] = False
            else:
                expiration = datetime.utcnow() + timedelta(seconds=SessionTable.TIME_OUT_LIMIT)
        if(not "_uid" in session):
            session["_uid"] = _create_identifier()
        SessionTable.newSession(session.sid,session,expiration)
        DynamoInterface.CountLimit = DynamoInterface.CountLimit + 1
        if DynamoInterface.CountLimit % DynamoInterface.SESSSION_CLEAR_COUNT_LIMIT == 0 :
            SessionTable.clearSessions()
            DynamoInterface.CountLimit = 1

        # Return session ID as header x-session-id
        response.headers["x-session-id"] = session.sid

class SessionTable :
    """
    This SessionTable is a Singletion Class/ Namespace used to wrap around
    AWS DynamoDB Boto.

    Constants :

    TABLE_NAME -- (String) The DynamoDB Table to be used for sessions
    KEY_NAME   -- (String) The hashed key name for the lookup
    DATA_FIELD -- (String) The field to store all data related to the session
    DATE_FIELD -- (String) The field used store the unix seconds since 1970
    TIME_OUT_LIMIT -- (int) The limit used for the session

    Static Fields :

    TableConnection -- (DynamoDBConnection)the conenction to the Sessions table
    isLocal -- (boolean) saves if the DynamoDB instances are local

    """
    TABLE_NAME = "BrokerSession"
    KEY_NAME = "uid"
    DATA_FIELD = "data"
    DATE_FIELD = "expiration"
    TIME_OUT_LIMIT = 604800
    LOCAL_PORT =  8000 # This is overwritten by the dynamo_port value taken from the configuration file
    TableConnection = ""
    isLocal = False
    DYNAMO_REGION = False

    @staticmethod
    def clearSessions() :
        """
        Removes old sessions that are expired
        """
        newTime = toUnixTime(datetime.utcnow())
        old_sessions = SessionTable.getTable().scan(expiration__lte=newTime)
        for recordItem in old_sessions :
            recordItem.delete()

    @staticmethod
    def getLocalConnection() :
        """
        returns the Boto DynamoDB connection object
        """
        return DynamoDBConnection(host='localhost', port=SessionTable.LOCAL_PORT, aws_access_key_id='a', aws_secret_access_key='a', is_secure=False)

    @staticmethod
    def getTable() :
        """
        returns the Boto Table object
        """
        if(SessionTable.isLocal) :
            return Table(SessionTable.TABLE_NAME,connection=SessionTable.getLocalConnection())
        return Table(SessionTable.TABLE_NAME,connection=connect_to_region(SessionTable.DYNAMO_REGION))



    @staticmethod
    def createTable(isLocal,localPort):
        """Used to create table for Dyanmo DB"""
        SessionTable.LOCAL_PORT =localPort
        secondaryIndex = [
            GlobalAllIndex('expiration-index',
                parts=[
                    HashKey('expiration', data_type=NUMBER)
                ],
                throughput={'read': 5, 'write': 5}
            )
        ]
        if isLocal:
            try:
                Table.create(
                    SessionTable.TABLE_NAME,
                    schema=[HashKey(SessionTable.KEY_NAME)],
                    global_indexes=secondaryIndex,
                    connection=SessionTable.getLocalConnection()
                )
            except exceptions.JSONResponseError as jre:
                if jre.status == 400 and "preexisting" in jre.message.lower():
                    #table already exists
                    pass

        else:
            Table.create(
                SessionTable.TABLE_NAME,
                schema=[HashKey(SessionTable.KEY_NAME)],
                global_indexes=secondaryIndex
            )

    @staticmethod
    def setup(app,isLocalHost):
        """
        Called when Flask starts to setup the connection infomation
        """
        SessionTable.isLocal = isLocalHost

    @staticmethod
    def doesSessionExist(uid) :
        """
        arguments:

        uid -- (String) the uid
        return (boolean) if the session
        """
        try:
            item = SessionTable.getTable().get_item(uid=uid)
            return True
        except :
            item = False
        return False

    @staticmethod
    def getTimeout(uid) :
        """
        arguments:

        uid -- (String) the uid
        return (int) time when the session expires
        """
        response = SessionTable.getTable().get_item(uid=uid)
        return response[SessionTable.DATE_FIELD]

    @staticmethod
    def getData(uid) :
        """
        uid -- (String) the uid
        return (Session) the session data
        """
        response = SessionTable.getTable().get_item(uid=uid)
        return response[SessionTable.DATA_FIELD]

    @staticmethod
    def newSession(uid,data,expiration) :
        """
        arguments:

        uid  -- (String) the session id
        data -- (String) the data for the session
        expiration -- (int) the time in seconds from 1970 when the session is no longer active

        Updates the exsiting session or creates a new one
        """
        if(SessionTable.doesSessionExist(uid)) :
            item = SessionTable.getTable().get_item(uid=uid)
            item[SessionTable.DATA_FIELD] = data
            item[SessionTable.DATE_FIELD] = toUnixTime(expiration)
            item.save(overwrite=True)
        else :
            SessionTable.getTable().put_item(data={
                SessionTable.KEY_NAME: uid,
                SessionTable.DATA_FIELD: data,
                SessionTable.DATE_FIELD: toUnixTime(expiration)
            })
