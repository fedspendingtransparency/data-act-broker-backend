from functools import wraps

from flask import session

from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactbroker.exceptions.invalid_usage import InvalidUsage
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PERMISSION_MAP, PERMISSION_TYPE_DICT_ID

NOT_AUTHORIZED_MSG = ("You are not authorized to perform the requested task. "
                      "Please contact your administrator.")

def permissions_check(f=None, permission=None):
    if permission is not None and permission not in PERMISSION_TYPE_DICT:
        raise ValueError("{} not a valid permission".format(permission))

    def actual_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                sess = GlobalDB.db().session
                error_message = "Login Required"
                if LoginSession.isLogin(session):
                    user = sess.query(User).filter(User.user_id == session["name"]).one()
                    valid_user = True

                    if permission is not None and not user.website_admin:
                        perm_hierarchy = {d['name']: d['order'] for d in PERMISSION_MAP.values()}
                        # if the users permission is not higher than the one specified, check their permission
                        # if user's perm order is < than what's passed in, it means they have higher permissions
                        if perm_hierarchy[PERMISSION_TYPE_DICT_ID[user.permission_type_id]] > perm_hierarchy[permission]:
                            if not user.permission_type_id == PERMISSION_TYPE_DICT[permission]:
                                valid_user = False

                    if valid_user:
                        return f(*args, **kwargs)
                    error_message = NOT_AUTHORIZED_MSG

                # No user logged in
                return JsonResponse.create(
                    StatusCode.LOGIN_REQUIRED,
                    {'message': error_message}
                )
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


def requires_admin(func):
    """Decorator requiring the requesting user be a website admin"""
    @wraps(func)
    def inner(*args, **kwargs):
        if not LoginSession.isLogin(session):
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': "Login Required"})

        sess = GlobalDB.db().session
        user = sess.query(User).\
            filter_by(user_id=session["name"]).one_or_none()
        if user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': "Login Required"})

        if not user.website_admin:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': NOT_AUTHORIZED_MSG})

        return func(*args, **kwargs)
    return inner
