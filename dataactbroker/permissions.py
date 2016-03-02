from functools import wraps
import json
import flask
from flask import session

from dataactbroker.handlers.aws.session import LoginSession

def permissions_check(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if LoginSession.isLogin(session):
            return f(*args, **kwargs)
        returnResponse = flask.Response()
        returnResponse.status_code = 401 # Error code
        responseDict = {}
        responseDict["message"] = "Login Required"
        returnResponse.set_data(json.dumps(responseDict))
        return returnResponse
    return decorated_function
