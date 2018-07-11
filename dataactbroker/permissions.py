from functools import wraps
from flask import g

from webargs import fields as webargs_fields
from webargs.flaskparser import parser as webargs_parser

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import SubTierAgency
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import (ALL_PERMISSION_TYPES_DICT, PERMISSION_SHORT_DICT, DABS_PERMISSION_ID_LIST,
                                        FABS_PERMISSION_ID_LIST)
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

NOT_AUTHORIZED_MSG = "You are not authorized to perform the requested task. Please contact your administrator."

DABS_PERMS = [PERMISSION_SHORT_DICT['w'], PERMISSION_SHORT_DICT['s']]
FABS_PERM = PERMISSION_SHORT_DICT['f']


def requires_login(func):
    """ Decorator requiring that a user be logged in (i.e. that we're not using an anonymous session)

        Args:
            func: the function that this wrapper is wrapped around

        Returns:
            LOGIN_REQUIRED JSONResponse object if the user doesn't exist, otherwise it runs the wrapped function
    """
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': "Login Required"})
        return func(*args, **kwargs)
    return inner


def requires_admin(func):
    """ Decorator requiring the requesting user be a website admin

        Args:
            func: the function that this wrapper is wrapped around

        Returns:
            LOGIN_REQUIRED JSONResponse object if the user doesn't exist or is not an admin user, otherwise it runs the
            wrapped function
    """
    @wraps(func)
    def inner(*args, **kwargs):
        if g.user is None:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': "Login Required"})

        if not g.user.website_admin:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED, {'message': NOT_AUTHORIZED_MSG})

        return func(*args, **kwargs)
    return inner


def current_user_can(permission, cgac_code=None, frec_code=None):
    """ Validate whether the current user can perform the act (described by the permission level) for the given
        cgac_code or frec_code

        Args:
            permission: single-letter string representing an application permission_type
            cgac_code: 3-digit numerical string identifying a CGAC agency
            frec_code: 4-digit numerical string identifying a FREC agency

        Returns:
            Boolean result on whether the user has permissions greater than or equal to permission
    """
    # If the user is not logged in, or the user is a website admin, there is no reason to check their permissions
    if not hasattr(g, 'user'):
        return False
    if g.user.website_admin:
        return True

    # Ensure the permission exists and retrieve its ID and type
    try:
        permission_id = ALL_PERMISSION_TYPES_DICT[permission]
    except KeyError:
        return False
    permission_list = FABS_PERMISSION_ID_LIST if permission_id in FABS_PERMISSION_ID_LIST else DABS_PERMISSION_ID_LIST

    # Loop through user's affiliations and return True if any match the permission
    for aff in g.user.affiliations:
        # Check if affiliation agency matches agency args
        if (aff.cgac and aff.cgac.cgac_code == cgac_code) or (aff.frec and aff.frec.frec_code == frec_code):
            # Check if affiliation has higher permissions than permission args
            aff_perm_id = aff.permission_type_id
            if (permission == 'reader') or (aff_perm_id in permission_list and aff_perm_id >= permission_id):
                return True

    return False


def current_user_can_on_submission(perm, submission, check_owner=True):
    """ Submissions add another permission possibility: if a user created a submission, they can do anything to it,
        regardless of submission agency

        Args:
            perm: string PermissionType value
            submission: Submission object
            check_owner: allows the functionality if the user is the owner of the Submission; default True

        Returns:
            Boolean result on whether the user has permissions greater than or equal to perm
    """
    is_owner = hasattr(g, 'user') and submission.user_id == g.user.user_id
    user_can = current_user_can(perm, cgac_code=submission.cgac_code, frec_code=submission.frec_code)
    return (is_owner and check_owner) or user_can


def requires_submission_perms(perm, check_owner=True, check_fabs=None):
    """ Decorator that checks the current user's permissions and validates that the submission exists. It expects a
        submission_id parameter on top of the function arguments.

        Args:
            perm: the type of permission we are checking for
            check_owner: a boolean indicating if we should check whether the user is the owner of the submission
            check_fabs: FABS permission to check if the Submission is FABS; default None

        Returns:
            A submission object obtained using the submission_id provided (along with the other args/kwargs that were
            initially provided)

        Raises:
            ResponseException: If the user doesn't have permission to access the submission at the level requested
                or the submission doesn't exist.
    """
    def inner(fn):
        @requires_login
        @wraps(fn)
        def wrapped(submission_id, *args, **kwargs):
            sess = GlobalDB.db().session
            submission = sess.query(Submission).filter_by(submission_id=submission_id).one_or_none()

            if submission is None:
                # @todo - why don't we use 404s?
                raise ResponseException('No such submission', StatusCode.CLIENT_ERROR)

            permission = check_fabs if check_fabs and submission.d2_submission else perm
            if not current_user_can_on_submission(permission, submission, check_owner):
                raise ResponseException("User does not have permission to access that submission",
                                        StatusCode.PERMISSION_DENIED)
            return fn(submission, *args, **kwargs)
        return wrapped
    return inner


def requires_agency_perms(perm):
    """ Decorator that checks the current user's permissions and validates them against the agency code. It expects an
         existing_submission_id, cgac_code, or frec_code parameter on top of the function arguments.

        Args:
            perm: the type of permission we are checking for

        Returns:
            The args/kwargs that were initially provided

        Raises:
            ResponseException: If the user doesn't have permission to access the submission at the level requested
                or no valid agency code was provided.
    """
    def inner(fn):
        @requires_login
        @wraps(fn)
        def wrapped(*args, **kwargs):
            sess = GlobalDB.db().session
            req_args = webargs_parser.parse({
                'existing_submission_id': webargs_fields.Int(missing=None),
                'cgac_code': webargs_fields.String(missing=None),
                'frec_code': webargs_fields.String(missing=None)
            })
            # Ensure there is either an existing_submission_id, a cgac_code, or a frec_code
            if req_args['existing_submission_id'] is None and req_args['cgac_code'] is None and \
               req_args['frec_code'] is None:
                raise ResponseException('No valid agency provided', StatusCode.CLIENT_ERROR)

            # Use codes based on existing Submission if existing_submission_id is provided, otherwise use CGAC or FREC
            if req_args['existing_submission_id'] is not None:
                submission = sess.query(Submission).\
                    filter(Submission.submission_id == req_args['existing_submission_id']).one_or_none()

                # Ensure submission exists
                if submission is None:
                    raise ResponseException('No valid agency provided', StatusCode.CLIENT_ERROR)

                # Check permissions for the submission
                if not current_user_can_on_submission(perm, submission):
                    raise ResponseException("User does not have permission to write to that agency",
                                            StatusCode.PERMISSION_DENIED)
            else:
                # Check permissions for the agency
                if not current_user_can(perm, cgac_code=req_args['cgac_code'], frec_code=req_args['frec_code']):
                    raise ResponseException("User does not have permission to write to that agency",
                                            StatusCode.PERMISSION_DENIED)
            return fn(*args, **kwargs)
        return wrapped
    return inner


def requires_sub_agency_perms(perm):
    """ Decorator that checks the current user's permissions and validates them against the agency code. It expects an
         agency_code parameter on top of the function arguments.

        Args:
            perm: the type of permission we are checking for

        Returns:
            The args/kwargs that were initially provided

        Raises:
            ResponseException: If the user doesn't have permission to access the submission at the level requested
                or no valid agency code was provided.
    """
    def inner(fn):
        @requires_login
        @wraps(fn)
        def wrapped(*args, **kwargs):
            sess = GlobalDB.db().session
            req_args = webargs_parser.parse({'agency_code': webargs_fields.String(missing=None)})

            # Retrieve agency codes based on SubTierAgency
            agency_code = req_args.get('agency_code', None)
            if agency_code:
                sub_tier_agency = sess.query(SubTierAgency).\
                    filter(SubTierAgency.sub_tier_agency_code == agency_code).one_or_none()
                cgac_code = sub_tier_agency.cgac.cgac_code if sub_tier_agency and sub_tier_agency.cgac_id else None
                frec_code = sub_tier_agency.frec.frec_code if sub_tier_agency and sub_tier_agency.frec_id else None

            if cgac_code is None and frec_code is None:
                raise ResponseException('No valid agency provided', StatusCode.CLIENT_ERROR)

            if not current_user_can(perm, cgac_code=cgac_code, frec_code=frec_code):
                raise ResponseException(
                    "User does not have '{}' permissions for SubTierAgency {}".format(perm, agency_code),
                    StatusCode.PERMISSION_DENIED)
            return fn(*args, **kwargs)
        return wrapped
    return inner


def separate_affiliations(affiliations, app_type):
    """ Separates CGAC and FREC UserAffiliations and removes affiliations with permissions outside of the specified
        application (FABS or DABS)

        Args:
            affiliations: list of UserAffiliations
            app_type: string deciding which application to use (FABS or DABS)

        Returns:
            A list of UserAffiliations with CGAC agencies within the app_type application
            A list of UserAffiliations with FREC agencies within the app_type application
    """
    cgac_ids, frec_ids = [], []
    app_permissions = FABS_PERMISSION_ID_LIST if app_type.lower() == 'fabs' else DABS_PERMISSION_ID_LIST

    for affiliation in affiliations:
        if affiliation.permission_type_id in app_permissions:
            if affiliation.frec:
                frec_ids.append(affiliation.frec.frec_id)
            else:
                cgac_ids.append(affiliation.cgac.cgac_id)

    return cgac_ids, frec_ids
