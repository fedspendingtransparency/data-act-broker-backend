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
    frec = FRECFactory(cgac=cgac)
    cgac_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(i), cgac=cgac, frec=None, is_frec=False,
                                           sub_tier_agency_name="Test Subtier Agency "+str(i)) for i in range(1)]
    frec_sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(1+i), cgac=frec_cgac, frec=frec, is_frec=True,
                                           sub_tier_agency_name="Test Subtier Agency "+str(1+i)) for i in range(1)]
    user.affiliations = [UserAffiliation(cgac=cgac, frec=None, permission_type_id=2),
                         UserAffiliation(cgac=None, frec=frec, permission_type_id=2)]
    sess.add_all([cgac] + [frec_cgac] + [frec] + cgac_sub_tiers + frec_sub_tiers + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    res = domain_app.get('/v1/list_agencies/').data.decode('UTF-8')
    res = json.loads(res)
    assert res['cgac_agency_list'] in [[{'agency_name': cgac.agency_name, 'cgac_code': cgac.cgac_code, 'priority': 1}],
                                       [{'agency_name': cgac.agency_name, 'cgac_code': cgac.cgac_code, 'priority': 2}]]
    assert res['frec_agency_list'] in [[{'agency_name': frec.agency_name, 'frec_code': frec.frec_code, 'priority': 1}],
                                       [{'agency_name': frec.agency_name, 'frec_code': frec.frec_code, 'priority': 2}]]


def test_list_agencies_superuser(database, monkeypatch, domain_app):
    """All agencies should be visible to website admins"""
    sess = GlobalDB.db().session
    user = UserFactory(website_admin=True)
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    sub_tiers = [SubTierAgencyFactory(sub_tier_agency_code=str(1230 + i), cgac=cgacs[i], frec=None, is_frec=False,
                                      sub_tier_agency_name="Test Subtier Agency "+str(i)) for i in range(3)]
    sess.add_all(cgacs + sub_tiers + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_agencies/').data.decode('UTF-8')
    result = json.loads(result)
    result = {el['cgac_code'] for el in result['cgac_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them


def test_list_agencies_all(monkeypatch, user_constants, domain_app):
    """All agencies should be visible to website admins"""
    sess = GlobalDB.db().session
    user = UserFactory()
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    frec_cgac = CGACFactory(cgac_code='3')
    frecs = [FRECFactory(frec_code=str(i), cgac=cgacs[i]) for i in range(3)]
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
    assert result == {'0', '1', '2'}  # i.e. all of them
    result = {el['frec_code'] for el in response['shared_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them
