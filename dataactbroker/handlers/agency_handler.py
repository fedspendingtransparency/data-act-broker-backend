from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, SubTierAgency

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


def get_sub_tiers_from_perms(is_admin, cgac_affil_ids, frec_affil_ids):
    sess = GlobalDB.db().session
    cgac_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(False))
    frec_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(True))

    # filter by user affiliations if user is not admin
    if not is_admin:
        cgac_sub_tier_agencies = cgac_sub_tier_agencies.filter(SubTierAgency.cgac_id.in_(cgac_affil_ids))
        frec_sub_tier_agencies = frec_sub_tier_agencies.filter(SubTierAgency.frec_id.in_(frec_affil_ids))

    return cgac_sub_tier_agencies, frec_sub_tier_agencies


def get_cgacs_without_sub_tier_agencies(sess=None):
    if sess is None:
        sess = GlobalDB.db().session
    cgac_sub_tiers = sess.query(SubTierAgency).distinct(SubTierAgency.cgac_id)
    return sess.query(CGAC).filter(CGAC.cgac_id.notin_([st.cgac.cgac_id for st in cgac_sub_tiers])).all()


def get_accessible_agencies(cgac_sub_tiers, frec_sub_tiers):
    """ List all CGAC and FREC Agencies user has DABS permissions for """
    # combine SubTierAgency CGACs with CGACs without SubTierAgencies into a cgac_list
    all_cgacs = [st.cgac for st in cgac_sub_tiers if st.is_frec is False] + get_cgacs_without_sub_tier_agencies()
    cgac_list = [{'agency_name': cst.agency_name, 'cgac_code': cst.cgac_code} for cst in all_cgacs]

    # convert the list of frec sub_tier_agencies into a list of frec agencies
    frec_list = [{'agency_name': fst.frec.agency_name, 'frec_code': fst.frec.frec_code} for fst in frec_sub_tiers]

    return JsonResponse.create(StatusCode.OK, {'cgac_agency_list': cgac_list, 'frec_agency_list': frec_list})


def get_all_agencies():
    """ List all CGAC and FREC agencies separately """
    sess = GlobalDB.db().session
    agency_list, shared_list = [], []

    # combine CGAC SubTierAgencies and CGACs without SubTierAgencies into the agency_list
    csubs = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(False)).distinct(SubTierAgency.cgac_id).all()
    all_cgacs = get_cgacs_without_sub_tier_agencies(sess) + [st.cgac for st in csubs if st.is_frec is False]
    agency_list = [{'agency_name': cst.agency_name, 'cgac_code': cst.cgac_code} for cst in all_cgacs]

    # add distinct FRECs from SubTierAgencies with a True is_frec into the shared_list
    fsubs = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(True)).distinct(SubTierAgency.frec_id).all()
    shared_list = [{'agency_name': fst.frec.agency_name, 'frec_code': fst.frec.frec_code} for fst in fsubs]

    return JsonResponse.create(StatusCode.OK, {'agency_list': agency_list, 'shared_agency_list': shared_list})
