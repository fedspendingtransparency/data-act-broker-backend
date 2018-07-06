from flask import g
from functools import wraps
from webargs import fields as webargs_fields
from webargs.flaskparser import parser as webargs_parser

from dataactbroker.handlers.agency_handler import get_sub_tiers_from_perms
from dataactbroker.permissions import requires_login, separate_affiliations

from dataactcore.models.domainModels import SubTierAgency
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


def convert_to_submission_id(fn):
    """ Decorator which reads the request, looking for a submission key to convert into a submission_id parameter. The
        provided function should have a submission_id parameter as its first argument.

        Returns:
            The submission ID that was found

        Raises:
            ResponseException: If a submission_id or submission parameter is not found
    """
    @wraps(fn)
    @requires_login     # check login before checking submission_id
    def wrapped(*args, **kwargs):
        req_args = webargs_parser.parse({
            'submission': webargs_fields.Int(),
            'submission_id': webargs_fields.Int()
        })
        submission_id = req_args.get('submission', req_args.get('submission_id'))
        if submission_id is None:
            raise ResponseException("submission_id is required", StatusCode.CLIENT_ERROR)
        return fn(submission_id, *args, **kwargs)
    return wrapped


def get_dabs_sub_tier_agencies(fn):
    """ Decorator which provides a list of all SubTierAgencies the user has DABS permissions for. The function should
    have a cgac_sub_tiers and a frec_sub_tiers parameter as its first argument. """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        cgac_sub_tiers, frec_sub_tiers = [], []
        if g.user is not None:
            # create list of affiliations
            cgac_ids, frec_ids = separate_affiliations(g.user.affiliations, 'dabs')

            # generate SubTierAgencies based on DABS permissions
            all_cgac_sub_tiers, all_frec_sub_tiers = get_sub_tiers_from_perms(g.user.website_admin, cgac_ids, frec_ids)

            # filter out copies of top-tier agencies
            cgac_sub_tiers = all_cgac_sub_tiers.distinct(SubTierAgency.cgac_id).all()
            frec_sub_tiers = all_frec_sub_tiers.distinct(SubTierAgency.frec_id).all()

        return fn(cgac_sub_tiers, frec_sub_tiers, *args, **kwargs)
    return wrapped


def get_fabs_sub_tier_agencies(fn):
    """ Decorator which provides a list of all SubTierAgencies the user has FABS permissions for. The function should
    have a sub_tier_agencies parameter as its first argument. """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        sub_tier_agencies = []
        if g.user is not None:
            # create list of affiliations
            cgac_ids, frec_ids = separate_affiliations(g.user.affiliations, 'fabs')

            # generate SubTierAgencies based on FABS permissions
            all_cgac_sub_tiers, all_frec_sub_tiers = get_sub_tiers_from_perms(g.user.website_admin, cgac_ids, frec_ids)
            sub_tier_agencies = all_cgac_sub_tiers.all() + all_frec_sub_tiers.all()

        return fn(sub_tier_agencies, *args, **kwargs)
    return wrapped
