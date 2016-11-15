import json
from functools import wraps

import flask
from flask import session

from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactbroker.exceptions.invalid_usage import InvalidUsage
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PERMISSION_MAP, PERMISSION_TYPE_DICT_ID

# This is used in conjunction with PERMISSION_TYPE_DICT to check if the permission passed in is part of the valid
# set of values we allow to be checked.
temp_perm_list = ["check_email_token", "check_password_token"]


def permissions_check(f=None,permission=None):

    # This will change to "if permission is not None and permission not in PERMISSION_TYPE_DICT" once the temp
    # perms above are removed during the local login refactor
    if permission is not None and permission not in list(PERMISSION_TYPE_DICT.keys()) + temp_perm_list:
        raise ValueError("{} not a valid permission".format(permission))

    def actual_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                sess = GlobalDB.db().session
                error_message = "Login Required"
                if permission == "check_email_token":
                    if LoginSession.isRegistering(session):
                        return f(*args, **kwargs)
                    else :
                        error_message = "unauthorized"
                elif permission == "check_password_token":
                    if LoginSession.isResetingPassword(session):
                        return f(*args, **kwargs)
                    else :
                        error_message  = "unauthorized"
                elif LoginSession.isLogin(session):
                    user = sess.query(User).filter(User.user_id == session["name"]).one()
                    valid_user = True

                    if permission is not None:
                        perm_hierarchy = {d['name']: d['order'] for d in PERMISSION_MAP.values()}
                        # if the users permission is not higher than the one specified, check their permission
                        # if user's perm order is < than what's passed in, it means they have higher permissions
                        if perm_hierarchy[PERMISSION_TYPE_DICT_ID[user.permission_type_id]] > perm_hierarchy[permission]:
                            if not user.permission_type_id == PERMISSION_TYPE_DICT[permission]:
                                valid_user = False

                    if valid_user:
                        return f(*args, **kwargs)
                    error_message = "Insufficient permissions to perform requested task."

                # No user logged in
                return_response = flask.Response()
                return_response.headers["Content-Type"] = "application/json"
                return_response.status_code = 401  # Error code
                response_dict = {}
                response_dict["message"] = error_message
                return_response.set_data(json.dumps(response_dict))
                return return_response

            except ResponseException as e:
                return JsonResponse.error(e,e.status)
            except InvalidUsage:
                raise
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
