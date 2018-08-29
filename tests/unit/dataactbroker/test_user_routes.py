import json
from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker import user_routes
from dataactbroker.handlers.account_handler import list_submission_users

from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from dataactcore.models.userModel import UserAffiliation

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
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


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_admin(database):
    """ Test listing all users with a submission (admin called the function) """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    admin_user = UserFactory(website_admin=True, name='Admin User')
    other_user = UserFactory.with_cgacs(cgacs[0], name='Test User')
    database.session.add_all(cgacs + [admin_user, other_user])
    database.session.commit()

    sub_1 = SubmissionFactory(cgac_code=cgacs[0].cgac_code, user_id=other_user.user_id, d2_submission=False)
    database.session.add(sub_1)
    database.session.commit()

    g.user = admin_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # Only lists users with submissions and doesn't care about affiliations because admin
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == other_user.user_id
    assert user_response[0]['name'] == other_user.name


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_cgac_affil(database):
    """ Test listing users based on cgac affiliations """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    first_user = UserFactory.with_cgacs(cgacs[0], name='Test User 1')
    other_user = UserFactory.with_cgacs(cgacs[1], name='Test User')
    database.session.add_all(cgacs + [first_user, other_user])
    database.session.commit()

    sub_1 = SubmissionFactory(cgac_code=cgacs[0].cgac_code, user_id=first_user.user_id, d2_submission=False)
    sub_2 = SubmissionFactory(cgac_code=cgacs[0].cgac_code, user_id=other_user.user_id, d2_submission=False)
    database.session.add_all([sub_1, sub_2])
    database.session.commit()

    g.user = first_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List both users because each has a submission with the cgac
    assert len(user_response) == 2
    assert {user_response[0]['user_id'], user_response[1]['user_id']} == {first_user.user_id, other_user.user_id}
    assert {user_response[0]['name'], user_response[1]['name']} == {first_user.name, other_user.name}

    g.user = other_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List only the submissions this user is part of because they have no cgac/frec affiliations with either submission
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == other_user.user_id
    assert user_response[0]['name'] == other_user.name


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_frec_affil(database):
    """ Test listing users based on frec affiliations """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    frecs = [FRECFactory(frec_code='0000', cgac=cgacs[0]), FRECFactory(frec_code='1111', cgac=cgacs[1])]
    first_user = UserFactory.with_cgacs(cgacs[0], name='Test User 1')
    other_user = UserFactory.with_cgacs(cgacs[1], name='Test User')
    third_user = UserFactory(name='Frec User')
    third_user.affiliations = [UserAffiliation(frec=frecs[0], user_id=third_user.user_id,
                                               permission_type_id=PERMISSION_TYPE_DICT['reader'])]
    database.session.add_all(cgacs + frecs + [first_user, other_user])
    database.session.commit()

    sub_1 = SubmissionFactory(frec_code=frecs[0].frec_code, user_id=first_user.user_id, d2_submission=False)
    sub_2 = SubmissionFactory(cgac_code=cgacs[1].cgac_code, user_id=other_user.user_id, d2_submission=False)
    sub_3 = SubmissionFactory(frec_code=frecs[1].frec_code, user_id=other_user.user_id, d2_submission=False)
    database.session.add_all([sub_1, sub_2, sub_3])
    database.session.commit()

    g.user = third_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List the first user because they have a submission with that frec
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == first_user.user_id
    assert user_response[0]['name'] == first_user.name


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_cgac_frec_affil(database):
    """ Test listing users based on both cgac and frec affiliations """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    frecs = [FRECFactory(frec_code='0000', cgac=cgacs[0]), FRECFactory(frec_code='1111', cgac=cgacs[1])]
    first_user = UserFactory.with_cgacs(cgacs[0], name='Test User 1')
    other_user = UserFactory.with_cgacs(cgacs[1], name='Test User')
    third_user = UserFactory.with_cgacs(cgacs[1], name='Frec User')
    third_user.affiliations =\
        third_user.affiliations + [UserAffiliation(frec=frecs[0], user_id=third_user.user_id,
                                                   permission_type_id=PERMISSION_TYPE_DICT['reader'])]
    database.session.add_all(cgacs + frecs + [first_user, other_user])
    database.session.commit()

    # Third user now has cgac 111 and frec 0000
    sub_1 = SubmissionFactory(frec_code=frecs[0].frec_code, user_id=first_user.user_id, d2_submission=False)
    sub_2 = SubmissionFactory(cgac_code=cgacs[1].cgac_code, user_id=other_user.user_id, d2_submission=False)
    sub_3 = SubmissionFactory(frec_code=frecs[1].frec_code, user_id=other_user.user_id, d2_submission=False)
    database.session.add_all([sub_1, sub_2, sub_3])
    database.session.commit()

    g.user = third_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List both other users because one has a frec agency and one has a cgac
    assert len(user_response) == 2
    assert {user_response[0]['user_id'], user_response[1]['user_id']} == {first_user.user_id, other_user.user_id}
    assert {user_response[0]['name'], user_response[1]['name']} == {first_user.name, other_user.name}


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_owned(database):
    """ Test listing users based on owned submissions """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    first_user = UserFactory.with_cgacs(cgacs[0], name='Test User 1')
    other_user = UserFactory.with_cgacs(cgacs[0], name='Test User')
    database.session.add_all(cgacs + [first_user, other_user])
    database.session.commit()

    sub_1 = SubmissionFactory(cgac_code=cgacs[1].cgac_code, user_id=other_user.user_id, d2_submission=False)
    database.session.add(sub_1)
    database.session.commit()

    g.user = first_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # Don't list any submissions because they don't own any and have no cgac/frec affiliations
    assert len(user_response) == 0

    g.user = other_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List the user because they have a submission they own (even though it doesn't match the cgac)
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == other_user.user_id
    assert user_response[0]['name'] == other_user.name


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("user_app")
def test_list_submission_users_fabs_dabs(database):
    """ Test listing DABS vs FABS users """
    cgacs = [CGACFactory(cgac_code='000'), CGACFactory(cgac_code='111')]
    first_user = UserFactory.with_cgacs(cgacs[0], name='Test User 1')
    other_user = UserFactory.with_cgacs(cgacs[1], name='Test User')
    database.session.add_all(cgacs + [first_user, other_user])
    database.session.commit()

    sub_1 = SubmissionFactory(cgac_code=cgacs[0].cgac_code, user_id=first_user.user_id, d2_submission=False)
    sub_2 = SubmissionFactory(cgac_code=cgacs[0].cgac_code, user_id=other_user.user_id, d2_submission=True)
    database.session.add_all([sub_1, sub_2])
    database.session.commit()

    g.user = first_user
    response = list_submission_users(False)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List only the first user because they're the only ones with a DABS submission
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == first_user.user_id
    assert user_response[0]['name'] == first_user.name

    response = list_submission_users(True)
    user_response = json.loads(response.data.decode('UTF-8'))['users']

    # List only the other user because they're the only ones with a FABS submission
    assert len(user_response) == 1
    assert user_response[0]['user_id'] == other_user.user_id
    assert user_response[0]['name'] == other_user.name
