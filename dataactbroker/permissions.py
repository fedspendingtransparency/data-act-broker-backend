from functools import wraps
import json
import flask
from flask import session

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.jsonResponse import JsonResponse
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.interfaceHolder import InterfaceHolder

def permissions_check(f=None,permissionList=[]):
    def actual_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                errorMessage  = "Login Required"
                if "check_email_token" in permissionList:
                    if(LoginSession.isRegistering(session)) :
                        return f(*args, **kwargs)
                    else :
                        errorMessage  = "unauthorized"
                elif "check_password_token" in permissionList  :
                    if(LoginSession.isResetingPassword(session)) :
                        return f(*args, **kwargs)
                    else :
                        errorMessage  = "unauthorized"
                elif LoginSession.isLogin(session):
                    userDb = UserHandler()
                    try:
                        user = userDb.getUserByUID(session["name"])
                        validUser = True
                        for permission in permissionList :
                            if(not userDb.hasPermission(user, permission)) :
                                validUser = False
                            else:
                                validUser = True
                                break

                    finally:
                        InterfaceHolder.closeOne(userDb)
                    if(validUser) :
                        return f(*args, **kwargs)
                    errorMessage  = "Wrong User Type"

                returnResponse = flask.Response()
                returnResponse.headers["Content-Type"] = "application/json"
                returnResponse.status_code = 401 # Error code
                responseDict = {}
                responseDict["message"] = errorMessage
                returnResponse.set_data(json.dumps(responseDict))
                return returnResponse
            except ResponseException as e:
                return JsonResponse.error(e,e.status)
            except Exception as e:
                exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
                return JsonResponse.error(exc,exc.status)
        return decorated_function
    if not f:
        def waiting_for_func(f):
            return actual_decorator(f)
        return waiting_for_func
    else:
        return actual_decorator(f)
