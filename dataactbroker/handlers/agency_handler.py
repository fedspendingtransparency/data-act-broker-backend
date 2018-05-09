from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, SubTierAgency


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