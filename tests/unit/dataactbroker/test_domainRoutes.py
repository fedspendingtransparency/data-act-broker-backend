import json
from unittest.mock import Mock

import pytest

from dataactbroker import domainRoutes
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import UserAffiliation
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory, SubTierAgencyFactory
from tests.unit.dataactcore.factories.user import UserFactory


@pytest.fixture
def domain_app(test_app):
    domainRoutes.add_domain_routes(test_app.application)
    yield test_app


def test_list_agencies_limits(monkeypatch, user_constants, domain_app):
    """List agencies should limit to only the user's agencies"""
    sess = GlobalDB.db().session
    user = UserFactory()
    cgac = CGACFactory()
    frec_cgac = CGACFactory()
    frec = FRECFactory(cgac=frec_cgac)
    sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code='0', cgac=cgac, frec=None, is_frec=False,
                                      sub_tier_agency_name="Test Subtier Agency 0"),
                 SubTierAgencyFactory(sub_tier_agency_code='1', cgac=frec_cgac, frec=frec, is_frec=True,
                                      sub_tier_agency_name="Test Subtier Agency 1")]
    user.affiliations = [UserAffiliation(cgac=cgac, frec=None, permission_type_id=2),
                         UserAffiliation(cgac=None, frec=frec, permission_type_id=2)]
    sess.add_all([cgac] + [frec_cgac] + [frec] + sub_tiers + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_agencies/').data.decode('UTF-8')
    res = json.loads(result)
    assert len(res['cgac_agency_list']) == 1
    assert len(res['frec_agency_list']) == 1
    assert res['cgac_agency_list'][0]['agency_name'] == cgac.agency_name
    assert res['cgac_agency_list'][0]['cgac_code'] == cgac.cgac_code
    assert res['frec_agency_list'][0]['agency_name'] == frec.agency_name
    assert res['frec_agency_list'][0]['frec_code'] == frec.frec_code


def test_list_agencies_superuser(database, monkeypatch, domain_app):
    """All agencies should be visible to website admins"""
    sess = GlobalDB.db().session
    user = UserFactory(website_admin=True)
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    frec_cgac = CGACFactory()
    frecs = [FRECFactory(frec_code=str(i), cgac=frec_cgac) for i in range(3)]
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgacs[i], frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency "+str(i)) for i in range(3)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(3+i), cgac=frec_cgac, frec=frecs[i], is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency "+str(3+i)) for i in range(3)]
    sess.add_all(cgacs + [frec_cgac] + frecs + cgac_sub_tiers + frec_sub_tiers + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_agencies/').data.decode('UTF-8')
    response = json.loads(result)
    result = {el['cgac_code'] for el in response['cgac_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them
    result = {el['frec_code'] for el in response['frec_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them


def test_list_agencies_all(monkeypatch, user_constants, domain_app):
    """All agencies should be visible to website admins"""
    sess = GlobalDB.db().session
    user = UserFactory()
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    frec_cgac = CGACFactory()
    frecs = [FRECFactory(frec_code=str(i), cgac=frec_cgac) for i in range(3)]
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgacs[i], frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency "+str(i)) for i in range(3)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(3+i), cgac=frec_cgac, frec=frecs[i], is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency "+str(3+i)) for i in range(3)]
    user.affiliations = [UserAffiliation(cgac=cgacs[0], frec=frecs[0], permission_type_id=2)]
    sess.add_all(cgacs + [frec_cgac] + frecs + cgac_sub_tiers + frec_sub_tiers + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_all_agencies/').data.decode('UTF-8')
    response = json.loads(result)
    result = {el['cgac_code'] for el in response['agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them
    result = {el['frec_code'] for el in response['shared_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them
