from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, SubTierAgency


def get_sub_tiers_from_perms(is_admin, cgac_affil_ids, frec_affil_ids):
    """ Gather the sub tier agencies associated with the given permissions or all if the user is an admin.

        Args:
            is_admin: a boolean indicating whether the user is an admin or not
            cgac_affil_ids: a list of IDs relating to cgacs the user is affiliated with
            frec_affil_ids: a list of IDs relating to frecs the user is affiliated with

        Returns:
            The cgac and frec sub tier agencies as SubTierAgency objects that the user is affiliated with as 2 return
            values (cgac then frec)
    """
    sess = GlobalDB.db().session
    cgac_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(False))
    frec_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(True))

    # filter by user affiliations if user is not admin
    if not is_admin:
        cgac_sub_tier_agencies = cgac_sub_tier_agencies.filter(SubTierAgency.cgac_id.in_(cgac_affil_ids))
        frec_sub_tier_agencies = frec_sub_tier_agencies.filter(SubTierAgency.frec_id.in_(frec_affil_ids))

    return cgac_sub_tier_agencies, frec_sub_tier_agencies


def get_cgacs_without_sub_tier_agencies(sess=None):
    """ Get a list of all cgac agencies that don't have an associated sub tier agency

        Args:
            sess: the current DB session

        Returns:
            A list of CGAC objects that do not have associated sub tier agencies.
    """
    if sess is None:
        sess = GlobalDB.db().session
    cgac_sub_tiers = sess.query(SubTierAgency).distinct(SubTierAgency.cgac_id)
    return sess.query(CGAC).filter(CGAC.cgac_id.notin_([st.cgac.cgac_id for st in cgac_sub_tiers])).all()


def get_accessible_agencies(cgac_sub_tiers, frec_sub_tiers):
    """ List all CGAC and FREC Agencies user has DABS permissions for

        Args:
            cgac_sub_tiers: a list of SubTierAgency objects
            frec_sub_tiers: a list of SubTierAgency objects

        Returns:
            A dictionary containing a list of all cgacs and frecs the user has access to.
    """
    # combine SubTierAgency CGACs with CGACs without SubTierAgencies into a cgac_list
    all_cgacs = [st.cgac for st in cgac_sub_tiers if st.is_frec is False] + get_cgacs_without_sub_tier_agencies()
    cgac_list = [{'agency_name': cst.agency_name, 'cgac_code': cst.cgac_code} for cst in all_cgacs]

    # convert the list of frec sub_tier_agencies into a list of frec agencies
    frec_list = [{'agency_name': fst.frec.agency_name, 'frec_code': fst.frec.frec_code} for fst in frec_sub_tiers]

    return {'cgac_agency_list': cgac_list, 'frec_agency_list': frec_list}


def get_all_agencies():
    """ List all CGAC and FREC agencies separately

        Returns:
            A dictionary containing all the CGAC agencies (as dictionaries of agency_name and cgac_code) under the
            "agency_list" key and all the FREC agencies (as dictionaries of agency_name and frec_code) under the
            "shared_agency_list" key.
    """
    sess = GlobalDB.db().session

    # combine CGAC SubTierAgencies and CGACs without SubTierAgencies into the agency_list
    csubs = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(False)).distinct(SubTierAgency.cgac_id).all()
    all_cgacs = get_cgacs_without_sub_tier_agencies(sess) + [st.cgac for st in csubs if st.is_frec is False]
    agency_list = [{'agency_name': cst.agency_name, 'cgac_code': cst.cgac_code} for cst in all_cgacs]

    # add distinct FRECs from SubTierAgencies with a True is_frec into the shared_list
    fsubs = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(True)).distinct(SubTierAgency.frec_id).all()
    shared_list = [{'agency_name': fst.frec.agency_name, 'frec_code': fst.frec.frec_code} for fst in fsubs]

    return {'agency_list': agency_list, 'shared_agency_list': shared_list}


def organize_sub_tier_agencies(sub_tier_agencies):
    """ Returns an organized list of sub tier agencies and their associated top tiers

        Args:
            sub_tier_agencies: list of SubTierAgency objects

        Returns:
            A dictionary containing a list of sub tier agencies organized in a human-readable way. Each dictionary
            in the list looks like this:

            {
              agency_name: Top-tier name: sub-tier name
              agency_code: sub-tier code
              priority: 2
            }
    """
    agencies = []
    for sub_tier in sub_tier_agencies:
        agency_name = sub_tier.frec.agency_name if sub_tier.is_frec else sub_tier.cgac.agency_name
        agencies.append({'agency_name': '{}: {}'.format(agency_name, sub_tier.sub_tier_agency_name),
                         'agency_code': sub_tier.sub_tier_agency_code, 'priority': sub_tier.priority})

    return {'sub_tier_agency_list': agencies}
