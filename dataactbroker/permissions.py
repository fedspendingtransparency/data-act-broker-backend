from functools import wraps

from flask import g

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.interfaces.db import GlobalDB
from dataactbroker.exceptions.invalid_usage import InvalidUsage
from dataactcore.models.lookups import PERMISSION_TYPE_DICT

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
                if g.user is not None:
                    valid_user = True

                    if permission is not None and not g.user.website_admin:
                        permission_id = PERMISSION_TYPE_DICT[permission]
                        if g.user.permission_type_id < permission_id:
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


def requires_login(func):
    """Decorator requiring that _a_ user be logged in (i.e. that we're not
    using an anonymous session)"""
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': "Login Required"})
        return func(*args, **kwargs)
    return inner


def requires_admin(func):
    """Decorator requiring the requesting user be a website admin"""
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': "Login Required"})

        if not g.user.website_admin:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': NOT_AUTHORIZED_MSG})

        return func(*args, **kwargs)
    return inner


def current_user_can(permission, cgac_code):
    """Can the current user perform the act (described by the permission
    level) for the given cgac_code?"""
    admin = hasattr(g, 'user') and g.user.website_admin
    has_affil = hasattr(g, 'user') and any(
        aff.cgac.cgac_code == cgac_code
        and aff.permission_type_id >= PERMISSION_TYPE_DICT[permission]
        for aff in g.user.affiliations
    )
    return admin or has_affil


def current_user_can_on_submission(perm, submission):
    """Submissions add another permission possibility: if a user created a
    submission, they can do anything to it, regardless of submission agency"""
    is_owner = hasattr(g, 'user') and submission.user_id == g.user.user_id
    return is_owner or current_user_can(perm, submission.cgac_code)
