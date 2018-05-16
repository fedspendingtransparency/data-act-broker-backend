import pytest

from dataactbroker.handlers.agency_handler import (
    get_sub_tiers_from_perms, get_cgacs_without_sub_tier_agencies, get_accessible_agencies, get_all_agencies,
    organize_sub_tier_agencies)

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory, SubTierAgencyFactory


@pytest.mark.usefixtures("user_constants")
def test_get_sub_tiers_from_perms(database):
    """ Test getting sub tiers of agencies from the permissions provided """
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    frec_cgac = CGACFactory()
    frecs = [FRECFactory(frec_code=str(i), cgac=frec_cgac) for i in range(3)]
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgacs[i], frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency "+str(i)) for i in range(3)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(3 + i), cgac=frec_cgac, frec=frecs[i], is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency " + str(3 + i)) for i in range(3)]
    database.session.add_all(cgacs + [frec_cgac] + frecs + cgac_sub_tiers + frec_sub_tiers)
    database.session.commit()

    # Test non-admin, should have 1 and 2 results for cgac and frec respectively
    cgac_result, frec_result = get_sub_tiers_from_perms(False, [cgacs[0].cgac_id], [frecs[0].frec_id, frecs[1].frec_id])
    assert cgac_result.count() == 1
    assert frec_result.count() == 2

    # Test non-admin with no permissions, should have 0 results for either
    cgac_result, frec_result = get_sub_tiers_from_perms(False, [], [])
    assert cgac_result.count() == 0
    assert frec_result.count() == 0

    # Test non-admin with permissions, should have 3 results for both (ignore permissions)
    cgac_result, frec_result = get_sub_tiers_from_perms(True, [cgacs[0].cgac_id], [frecs[0].frec_id, frecs[1].frec_id])
    assert cgac_result.count() == 3
    assert frec_result.count() == 3

    # Test non-admin with no permissions, should have 3 results for both
    cgac_result, frec_result = get_sub_tiers_from_perms(True, [], [])
    assert cgac_result.count() == 3
    assert frec_result.count() == 3


def test_get_cgacs_without_sub_tier_agencies(database):
    """ Test getting all cgacs without any sub tier agencies """
    sub_tier_cgac = CGACFactory()
    no_sub_cgac = CGACFactory()
    sub_tier = SubTierAgencyFactory(sub_tier_agency_code="0", cgac=sub_tier_cgac, frec=None, is_frec=False,
                                    sub_tier_agency_name="Test Subtier Agency")
    database.session.add_all([sub_tier_cgac] + [no_sub_cgac] + [sub_tier])
    database.session.commit()

    # Test while passing the session
    results = get_cgacs_without_sub_tier_agencies(database.session)
    assert len(results) == 1
    assert results[0].cgac_id == no_sub_cgac.cgac_id

    # Test without passing the session
    results = get_cgacs_without_sub_tier_agencies()
    assert len(results) == 1
    assert results[0].cgac_id == no_sub_cgac.cgac_id


def test_get_accessible_agencies(database):
    """ Test listing all the agencies (CGAC and FREC) that are accessible based on permissions given """
    cgacs = [CGACFactory(cgac_code=str(i), agency_name="Test Agency " + str(i)) for i in range(3)]
    frec_cgac = CGACFactory()
    frecs = [FRECFactory(frec_code=str(i), cgac=frec_cgac) for i in range(3)]
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgacs[i], frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency " + str(i)) for i in range(3)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(3 + i), cgac=frec_cgac, frec=frecs[i], is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency " + str(3 + i)) for i in range(3)]
    database.session.add_all(cgacs + [frec_cgac] + frecs + cgac_sub_tiers + frec_sub_tiers)
    database.session.commit()

    # Test one CGAC and 2 FRECs, have to decode it because we send it back as a response already
    results = get_accessible_agencies([cgac_sub_tiers[0]], [frec_sub_tiers[0], frec_sub_tiers[2]])
    frec_code_result = {el["frec_code"] for el in results["frec_agency_list"]}
    frec_name_result = {el["agency_name"] for el in results["frec_agency_list"]}
    assert len(results["cgac_agency_list"]) == 1
    assert len(results["frec_agency_list"]) == 2
    assert results["cgac_agency_list"][0]["agency_name"] == cgacs[0].agency_name
    assert results["cgac_agency_list"][0]["cgac_code"] == cgacs[0].cgac_code
    assert frec_name_result == {frecs[0].agency_name, frecs[2].agency_name}
    assert frec_code_result == {frecs[0].frec_code, frecs[2].frec_code}

    # Test when there are no FRECs
    results = get_accessible_agencies([cgac_sub_tiers[0]], [])
    assert len(results["cgac_agency_list"]) == 1
    assert len(results["frec_agency_list"]) == 0


def test_get_all_agencies(database):
    """ Test printing out all agencies, FRECs only retrieved if they have a sub tier, CGACs always """
    cgacs = [CGACFactory(cgac_code=str(i), agency_name="Test Agency " + str(i)) for i in range(3)]
    frec_cgac = CGACFactory()
    frecs = [FRECFactory(frec_code=str(i), cgac=frec_cgac) for i in range(6, 9)]
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgacs[i], frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency " + str(i)) for i in range(2)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(3 + i), cgac=frec_cgac, frec=frecs[i], is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency " + str(3 + i)) for i in range(2)]
    database.session.add_all(cgacs + [frec_cgac] + frecs + cgac_sub_tiers + frec_sub_tiers)
    database.session.commit()

    results = get_all_agencies()
    cgac_result = {el["cgac_code"] for el in results["agency_list"]}
    frec_result = {el["frec_code"] for el in results["shared_agency_list"]}
    assert len(results["agency_list"]) == 3
    assert len(results["shared_agency_list"]) == 2
    assert cgac_result == {"0", "1", "2"}
    assert frec_result == {"6", "7"}


def test_organize_sub_tier_agencies(database):
    """ Test organization of passed sub tier agencies """
    cgac = CGACFactory()
    frec_cgac = CGACFactory()
    frec = FRECFactory(cgac=frec_cgac)
    sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code='0', cgac=cgac, frec=None, is_frec=False,
                                      sub_tier_agency_name="Test Subtier Agency 0"),
                 SubTierAgencyFactory(sub_tier_agency_code='1', cgac=frec_cgac, frec=frec, is_frec=True,
                                      sub_tier_agency_name="Test Subtier Agency 1")]
    database.session.add_all([cgac] + [frec_cgac] + [frec] + sub_tiers)
    database.session.commit()

    # Test with no sub tiers
    results = organize_sub_tier_agencies([])
    assert len(results["sub_tier_agency_list"]) == 0

    # Test with just one sub tier passed in
    results = organize_sub_tier_agencies([sub_tiers[0]])
    assert len(results["sub_tier_agency_list"]) == 1
    assert results["sub_tier_agency_list"][0] == {
        "agency_name": '{}: {}'.format(cgac.agency_name, sub_tiers[0].sub_tier_agency_name),
        "agency_code": sub_tiers[0].sub_tier_agency_code,
        "priority": sub_tiers[0].priority}

    # Test with both sub tiers passed
    results = organize_sub_tier_agencies([sub_tiers[0], sub_tiers[1]])
    assert len(results["sub_tier_agency_list"]) == 2
