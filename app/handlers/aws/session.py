from uuid import uuid4
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table, exceptions
from datetime import datetime, timedelta
from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

#
# Wraper for Session Dictionary
#
class LoginSession():

    @staticmethod
    def getName(session) :
        print str(session)
        if session.get('name') is not None :
            return session['name']
        return ""

    @staticmethod
    def isLogin(session) :
        if session.get('login') is not None :
            return True
        return False

    @staticmethod
    def logout(session) :
        session.pop("login", None)
        session.pop("name", None)

    @staticmethod
    def login(session,username) :
        session["name"] =  username
        session["login"] = True
 #
 #@uid the session id
 #
@staticmethod
def getData(uid) :
    response = SessionTable.getTable().get_item(uid=uid)
    return response[SessionTable.DATA_FIELD]

#Converts datetimeValue to time in seconds ince 1970
#returns int
#

def toUnixTime(datetimeValue) :
  return (datetimeValue-datetime(1970,1,1)).total_seconds()

#
#Class that wraps around normal Flask Session object
#
class DynamoSession(CallbackDict, SessionMixin):

    def __init__(self, initial=None, sid=None):
        CallbackDict.__init__(self, initial)
        self.sid = sid
        self.modified = False

#
#Class That implements the SessionInterface and uses SessionTable to store data
#
class DynamoInterface(SessionInterface):

    def __init__(self):
        return

    #
    #@app the Flask applcation
    #@request the request object
    #
    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if(sid and SessionTable.doesSessionExist(sid)):
            if SessionTable.getTimeout(sid)> toUnixTime(datetime.utcnow()):
                return DynamoSession(initial=SessionTable.getData(sid),sid=sid)
        # This can be made better most likely need to do research
        # Maybe Hash(time + server id + random number)? Want to prevent any conflicts
        sid = str(uuid4())

        return DynamoSession(sid=sid)

    #
    #@app the Flask applcation
    #@request the request object
    #@session the Flask session object
    #
    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if not session:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return
        if self.get_expiration_time(app, session):
            expiration = self.get_expiration_time(app, session)
        else:
            expiration = datetime.utcnow() + timedelta(seconds=SessionTable.TIME_OUT_LIMIT)
        SessionTable.newSession(session.sid,session,expiration)

        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=self.get_expiration_time(app, session),
                            httponly=True, domain=domain)

#
# This SessionTable is a Singletion Class/ Namespace used to wrap around
# AWS DynamoDB Boto.

# To aide in portablity the following constants are used

# @TABLE_NAME The DynamoDB Table to be used for sessions
# @KEY_NAME The hashed key name for the lookup
# @DATA_FIELD The field to store all data related to the session
# @DATE_FIELD The field used store the unix seconds since 1970
# @TIME_OUT_LIMIT The limit used for the session

# Static fields
# @TableConnection the conenction to the Sessions table
# @local saves if the DynamoDB instances are local
#
class SessionTable :
    TABLE_NAME = "BrokerSession"
    KEY_NAME = "uid"
    DATA_FIELD = "data"
    DATE_FIELD = "expiration"
    TIME_OUT_LIMIT = 30
    TableConnection = ""
    isLocal = False

    #
    #returns the Boto DynamoDB connection object
    #
    @staticmethod
    def getLocalConnection() :
        return DynamoDBConnection(host='localhost',port=8000,aws_access_key_id='a',aws_secret_access_key='a',is_secure=False)

    #
    #returns the Boto Table object
    #
    @staticmethod
    def getTable() :
        if(SessionTable.isLocal) :
            return Table(SessionTable.TABLE_NAME,connection=SessionTable.getLocalConnection())
        return Table(SessionTable.TABLE_NAME)

    #
    #Called when Flask starts to setup the connection infomation
    #
    @staticmethod
    def setup(app,isLocalHost,createTables):
        SessionTable.isLocal = isLocalHost
        if(createTables and isLocalHost) :
            TableConnection = Table.create(SessionTable.TABLE_NAME,schema=[HashKey(SessionTable.KEY_NAME)],connection=SessionTable.getLocalConnection())

    #
    #@uid the session id
    #
    @staticmethod
    def doesSessionExist(uid) :
        try:
            item = SessionTable.getTable().get_item(uid=uid)
            return True
        except :
            item = False
        return False

    #
    #@uid the session id
    #
    @staticmethod
    def getTimeout(uid) :
        response = SessionTable.getTable().get_item(uid=uid)
        return response[SessionTable.DATE_FIELD]

    #
    #@uid the session id
    #
    @staticmethod
    def getData(uid) :
        response = SessionTable.getTable().get_item(uid=uid)
        return response[SessionTable.DATA_FIELD]

    #
    #Updates the exsiting session or creates a new one
    #@uid the session id
    #@data the data for the session
    #@expiration the time in seconds from 1970 when the session is no longer active
    #
    @staticmethod
    def newSession(uid,data,expiration) :
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
