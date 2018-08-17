import json
from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker import user_routes
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.user import UserFactory


@pytest.fixture
def user_app(test_app):
    user_routes.add_user_routes(test_app.application, Mock(), Mock())
    yield test_app


@pytest.mark.usefixtures("user_constants")
def test_list_user_emails(database, user_app):
    """ Test listing user emails """
    cgacs = [CGACFactory() for _ in range(3)]
    users = [UserFactory.with_cgacs(cgacs[0]),
             UserFactory.with_cgacs(cgacs[0], cgacs[1]),
             UserFactory.with_cgacs(cgacs[1]),
             UserFactory.with_cgacs(cgacs[2])]
    database.session.add_all(users)
    database.session.commit()

    def user_ids():
        result = user_app.get('/v1/list_user_emails/').data.decode('UTF-8')
        return {user['id'] for user in json.loads(result)['users']}

    g.user = users[0]
    assert user_ids() == {users[0].user_id, users[1].user_id}
    g.user = users[3]
    assert user_ids() == {users[3].user_id}

    g.user.website_admin = True
    database.session.commit()
    assert user_ids() == {user.user_id for user in users}
