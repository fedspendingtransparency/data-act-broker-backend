import json
from unittest.mock import Mock

import pytest

from dataactbroker import domainRoutes
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import UserAffiliation
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.user import UserFactory


@pytest.fixture
def domain_app(test_app):
    domainRoutes.add_domain_routes(test_app.application)
    yield test_app


def test_list_agencies_limits(monkeypatch, user_constants, domain_app):
    """List agencies should limit to only the user's agencies"""
    sess = GlobalDB.db().session
    user = UserFactory()
    cgacs = [CGACFactory() for _ in range(2)]
    user.affiliations = [UserAffiliation(cgac=cgacs[0], permission_type_id=2)]
    sess.add_all(cgacs + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_agencies/').data.decode('UTF-8')
    result = json.loads(result)
    assert result['cgac_agency_list'] == [{'agency_name': cgacs[0].agency_name, 'cgac_code': cgacs[0].cgac_code}]


def test_list_agencies_superuser(database, monkeypatch, domain_app):
    """All agencies should be visible to website admins"""
    sess = GlobalDB.db().session
    user = UserFactory(website_admin=True)
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(3)]
    sess.add_all(cgacs + [user])
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
    user.affiliations = [UserAffiliation(cgac=cgacs[0], permission_type_id=2)]
    sess.add_all(cgacs + [user])
    sess.commit()
    monkeypatch.setattr(domainRoutes, 'g', Mock(user=user))

    result = domain_app.get('/v1/list_all_agencies/').data.decode('UTF-8')
    result = json.loads(result)
    result = {el['cgac_code'] for el in result['cgac_agency_list']}
    assert result == {'0', '1', '2'}    # i.e. all of them
