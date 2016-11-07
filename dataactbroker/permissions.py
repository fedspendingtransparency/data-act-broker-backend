import json
from functools import wraps

import flask
from flask import session

from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.handlers.userHandler import UserHandler
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import has_permission
from dataactcore.models.userModel import User


def permissions_check(f=None,permission_list=[]):
    def actual_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                sess = GlobalDB.db().session
                error_message  = "Login Required"
                if "check_email_token" in permission_list:
                    if LoginSession.isRegistering(session):
                        return f(*args, **kwargs)
                    else :
                        error_message  = "unauthorized"
                elif "check_password_token" in permission_list  :
                    if LoginSession.isResetingPassword(session):
                        return f(*args, **kwargs)
                    else :
                        error_message  = "unauthorized"
                elif LoginSession.isLogin(session):
                    user_db = UserHandler()
                    try:
                        user = sess.query(User).filter(User.user_id == session["name"]).one()
                        valid_user = True
                        for permission in permission_list :
                            if not has_permission(user, permission):
                                valid_user = False
                            else:
                                valid_user = True
                                break

                    finally:
                        user_db.close()
                    if valid_user:
                        return f(*args, **kwargs)
                    error_message  = "Wrong User Type"

                # No user logged in
                return_response = flask.Response()
                return_response.headers["Content-Type"] = "application/json"
                return_response.status_code = 401 # Error code
                response_dict = {}
                response_dict["message"] = error_message
                return_response.set_data(json.dumps(response_dict))
                return return_response

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
