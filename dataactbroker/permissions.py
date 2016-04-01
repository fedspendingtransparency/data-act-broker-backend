from functools import wraps
import json
import flask
from flask import session

from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.interfaceHolder import InterfaceHolder

def permissions_check(f=None,permissionList=[]):
    def actual_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            errorMessage  = "Login Required"
            #Check if address and browser match what is stored
            if(not LoginSession.isSessionSecure(session) ):
                # log user out and reset
                LoginSession.logout(session)
                LoginSession.resetID(session)
                errorMessage  = "Session Error"
            else :
                if LoginSession.isLogin(session):
                    userDb = UserHandler()
                    try:
                        user = userDb.getUserByUID(session["name"])
                        validUser = True
                        for permission in permissionList :
                            if(not userDb.hasPermisson(user,permission) ) :
                                validUser = False
                    finally:
                        InterfaceHolder.closeOne(userDb)
                    if(validUser) :
                        return f(*args, **kwargs)
                    errorMessage  = "Wrong User Type"
                elif "check_email_token" in permissionList:
                    if(LoginSession.isRegistering(session)) :
                        return f(*args, **kwargs)
                    else :
                        errorMessage  = "unauthorized"
                elif "check_password_token" in permissionList  :
                    if(LoginSession.isResetingPassword(session)) :
                        return f(*args, **kwargs)
                    else :
                        errorMessage  = "unauthorized"

            returnResponse = flask.Response()
            returnResponse.headers["Content-Type"] = "application/json"
            returnResponse.status_code = 401 # Error code
            responseDict = {}
            responseDict["message"] = errorMessage
            returnResponse.set_data(json.dumps(responseDict))
            return returnResponse
        return decorated_function
    if not f:
        def waiting_for_func(f):
            return actual_decorator(f)
        return waiting_for_func
    else:
        return actual_decorator(f)
