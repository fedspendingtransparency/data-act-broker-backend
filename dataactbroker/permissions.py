from functools import wraps

from flask import g

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PERMISSION_SHORT_DICT
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

NOT_AUTHORIZED_MSG = ("You are not authorized to perform the requested task. "
                      "Please contact your administrator.")


def requires_login(func):
    """Decorator requiring that _a_ user be logged in (i.e. that we're not using an anonymous session)"""
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': "Login Required"})
        return func(*args, **kwargs)
    return inner


def requires_admin(func):
    """Decorator requiring the requesting user be a website admin"""
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': "Login Required"})

        if not g.user.website_admin:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': NOT_AUTHORIZED_MSG})

        return func(*args, **kwargs)
    return inner


def current_user_can(perm, cgac_code, frec_code):
    """Can the current user perform the act (described by the perm level) for the given cgac_code or frec_code?"""
    admin = hasattr(g, 'user') and g.user.website_admin
    matching_perms = []
    try:
        permission = PERMISSION_TYPE_DICT[perm]
    except KeyError:
        permission = 999

    for aff in g.user.affiliations:
        # affiliation matches agency args
        agency = (aff.cgac and aff.cgac.cgac_code == cgac_code) or (aff.frec and aff.frec.frec_code == frec_code)
        # affiliation permissions are FABS
        fabs = aff.permission_type_id == PERMISSION_SHORT_DICT['f']
        # affiliation has permission higher than perm args
        dabs_perms = aff.permission_type_id >= permission

        if agency and ((dabs_perms and not fabs) or (perm == 'fabs' and fabs)):
            matching_perms.append(aff)

    has_affil = hasattr(g, 'user') and len(matching_perms)
    return admin or has_affil


def current_user_can_on_submission(perm, submission, check_owner=True):
    """Submissions add another permission possibility: if a user created a submission, they can do anything to it,
    regardless of submission agency"""
    is_owner = hasattr(g, 'user') and submission.user_id == g.user.user_id
    return (is_owner and check_owner) or current_user_can(perm, submission.cgac_code, submission.frec_code)


def requires_submission_perms(perm, check_owner=True):
    """Decorator that checks the current user's permissions and validates that the submission exists. It expects a
    submission_id parameter and will return a submission object"""
    def inner(fn):
        @requires_login
        @wraps(fn)
        def wrapped(submission_id, *args, **kwargs):
            sess = GlobalDB.db().session
            submission = sess.query(Submission).\
                filter_by(submission_id=submission_id).one_or_none()

            if submission is None:
                # @todo - why don't we use 404s?
                raise ResponseException('No such submission',
                                        StatusCode.CLIENT_ERROR)

            if not current_user_can_on_submission(perm, submission, check_owner):
                raise ResponseException(
                    "User does not have permission to access that submission",
                    StatusCode.PERMISSION_DENIED)
            return fn(submission, *args, **kwargs)
        return wrapped
    return inner
