import json
import pytest
from unittest.mock import Mock

from dataactbroker.handlers import account_handler
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PERMISSION_SHORT_DICT
from dataactcore.models.userModel import UserAffiliation
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.user import UserFactory


def make_caia_token_dict(unique_id):
    """We need to create mock CAIA data at multiple points in these tests"""
    return {
        "access_token": f"access-token-{unique_id}",
        'refresh_token': f'refresh-token-{unique_id}',
        'id_token': f'id-token-{unique_id}',
        'token_type': 'Bearer',
        'expires_in': 7200
    }


def make_caia_user_dict(role_str):
    """We need to create mock CAIA data at multiple points in these tests"""
    return {
        "sub": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "role": role_str,
        "orgname": "",
        "given_name": "test",
        "middle_name": "",
        "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "family_name": "user",
        "email": "test-user@email.com"
    }


@pytest.mark.usefixtures("user_constants")
def test_caia_login_success_normal_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = False
    mock_dict.return_value.safeDictionary.side_effect = {'code': '', 'redirect_url': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    caia_tokens_dict = make_caia_token_dict('123456789')
    monkeypatch.setattr(account_handler, 'get_caia_tokens', Mock(return_value=caia_tokens_dict))

    monkeypatch.setattr(account_handler, 'revoke_caia_access', Mock())

    # Testing with just the admin role - note that with a singular role, CAIA drops the brackets
    caia_user_dict = make_caia_user_dict('admin')
    monkeypatch.setattr(account_handler, 'get_caia_user_dict', Mock(return_value=caia_user_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.caia_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']

    # Testing with several roles
    caia_user_dict = make_caia_user_dict('[admin, CGAC-123-R,'
                                         ' AppApprover-Data_Act_Broker, AppOwner-Data_Act_Broker-CGAC-123]')
    monkeypatch.setattr(account_handler, 'get_caia_user_dict', Mock(return_value=caia_user_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.caia_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_caia_login_failure_normal_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = False
    mock_dict.return_value.safeDictionary.side_effect = {'code': '', 'redirect_uri': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    caia_tokens_dict = {}
    monkeypatch.setattr(account_handler, 'get_caia_tokens', Mock(return_value=caia_tokens_dict))
    json_response = ah.caia_login(Mock())
    error_message = ("The CAIA endpoint was unable to locate your session "
                     "using the code/redirect_uri combination you provided.")

    # Did not get a successful response from CAIA
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_set_user_name_updated():
    """ Tests set_user_name()  updates a user's name """

    user = UserFactory(name="No User")

    account_handler.set_user_name(user, 'New', '', 'Name')

    assert user.name == 'New Name'


def test_set_user_name_middle_name():
    """ Tests set_user_name() adds a middle name initial to the user's name when a middle name is provided """

    user = UserFactory()

    account_handler.set_user_name(user, 'Test', 'Abc', 'User')

    assert user.name == 'Test A. User'


def test_set_user_name_empty_middle_name():
    """ Tests set_user_name() omits a middle name initial to the user's name when a middle name is empty (spaces) """
    user = UserFactory()

    account_handler.set_user_name(user, 'Test', ' ', 'User')

    assert user.name == 'Test User'


def test_set_user_name_no_middle_name():
    """ Tests set_user_name() omits a middle name initial to the user's name when a middle name is empty (None) """

    user = UserFactory()

    account_handler.set_user_name(user, 'Test', None, 'User')

    assert user.name == 'Test User'


@pytest.mark.usefixtures("user_constants")
def test_set_caia_perms(database):
    """Verify that we get the _highest_ permission within our CGAC"""
    cgac_abc = CGACFactory(cgac_code='ABC')
    cgac_def = CGACFactory(cgac_code='DEF')
    frec_abc = FRECFactory(frec_code='ABCD', cgac=cgac_abc)
    frec_abc2 = FRECFactory(frec_code='EFGH', cgac=cgac_abc)
    frec_def = FRECFactory(frec_code='IJKL', cgac=cgac_def)
    user = UserFactory()
    database.session.add_all([cgac_abc, cgac_def, frec_abc, frec_abc2, frec_def, user])
    database.session.commit()

    # test creating permission from string
    account_handler.set_caia_perms(user, ['CGAC-ABC-W'])
    database.session.commit()   # populate ids
    assert len(user.affiliations) == 1
    affil = user.affiliations[0]
    assert affil.cgac_id == cgac_abc.cgac_id
    assert affil.permission_type_id == PERMISSION_TYPE_DICT['writer']

    # test creating CGAC permission from two strings
    account_handler.set_caia_perms(user, ['CGAC-ABC-R', 'CGAC-ABC-S'])
    database.session.commit()   # populate ids
    assert len(user.affiliations) == 1
    affil = user.affiliations[0]
    assert affil.cgac_id == cgac_abc.cgac_id
    assert affil.permission_type_id == PERMISSION_TYPE_DICT['submitter']

    # test creating two CGAC permissions with two strings
    account_handler.set_caia_perms(user, ['CGAC-ABC-R', 'CGAC-DEF-S'])
    database.session.commit()
    assert len(user.affiliations) == 2
    affiliations = list(sorted(user.affiliations, key=lambda a: a.cgac.cgac_code))
    abc_aff, def_aff = affiliations
    assert abc_aff.cgac.cgac_code == 'ABC'
    assert abc_aff.frec is None
    assert abc_aff.permission_type_id == PERMISSION_TYPE_DICT['reader']
    assert def_aff.cgac.cgac_code == 'DEF'
    assert def_aff.frec is None
    assert def_aff.permission_type_id == PERMISSION_TYPE_DICT['submitter']

    # test creating FREC permission from two strings
    account_handler.set_caia_perms(user, ['FREC-ABCD-R', 'FREC-ABCD-S'])
    database.session.commit()
    assert len(user.affiliations) == 2
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    assert frec_affils[0].cgac is None
    assert frec_affils[0].frec.frec_code == 'ABCD'
    assert frec_affils[0].permission_type_id == PERMISSION_TYPE_DICT['submitter']
    cgac_affils = [affil for affil in user.affiliations if affil.cgac is not None]
    assert cgac_affils[0].cgac.cgac_code == 'ABC'
    assert cgac_affils[0].frec is None
    assert cgac_affils[0].permission_type_id == PERMISSION_TYPE_DICT['reader']

    # test creating two FREC permissions from two strings
    account_handler.set_caia_perms(user, ['FREC-ABCD-R', 'FREC-EFGH-S'])
    database.session.commit()
    assert len(user.affiliations) == 3
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    frec_affiliations = list(sorted(frec_affils, key=lambda a: a.frec.frec_code))
    abcd_aff, efgh_aff = frec_affiliations
    assert abcd_aff.cgac is None
    assert abcd_aff.frec.frec_code == 'ABCD'
    assert abcd_aff.permission_type_id == PERMISSION_TYPE_DICT['reader']
    assert efgh_aff.cgac is None
    assert efgh_aff.frec.frec_code == 'EFGH'
    assert efgh_aff.permission_type_id == PERMISSION_TYPE_DICT['submitter']
    cgac_affils = [affil for affil in user.affiliations if affil.cgac is not None]
    assert cgac_affils[0].cgac.cgac_code == 'ABC'
    assert cgac_affils[0].frec is None
    assert cgac_affils[0].permission_type_id == PERMISSION_TYPE_DICT['reader']

    # test creating one CGAC and one FREC permission from two strings
    account_handler.set_caia_perms(user, ['CGAC-ABC-S', 'FREC-IJKL-R'])
    database.session.commit()
    assert len(user.affiliations) == 3
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    assert frec_affils[0].cgac is None
    assert frec_affils[0].frec.frec_code == 'IJKL'
    assert frec_affils[0].permission_type_id == PERMISSION_TYPE_DICT['reader']
    cgac_affils = [affil for affil in user.affiliations if affil.cgac is not None]
    cgac_affiliations = list(sorted(cgac_affils, key=lambda a: a.cgac.cgac_code))
    abc_aff, def_aff = cgac_affiliations
    assert abc_aff.cgac.cgac_code == 'ABC'
    assert abc_aff.frec is None
    assert abc_aff.permission_type_id == PERMISSION_TYPE_DICT['submitter']
    assert def_aff.cgac.cgac_code == 'DEF'
    assert def_aff.frec is None
    assert def_aff.permission_type_id == PERMISSION_TYPE_DICT['reader']

    # test creating DABS and FABS CGAC permissions from three strings
    account_handler.set_caia_perms(user, ['CGAC-ABC-R', 'CGAC-ABC-S', 'CGAC-ABC-F'])
    database.session.commit()
    assert len(user.affiliations) == 2
    affiliations = list(sorted(user.affiliations, key=lambda a: a.permission_type_id))
    dabs_aff, fabs_aff = affiliations
    assert dabs_aff.cgac.cgac_code == 'ABC'
    assert dabs_aff.frec is None
    assert dabs_aff.permission_type_id == PERMISSION_TYPE_DICT['submitter']
    assert fabs_aff.cgac.cgac_code == 'ABC'
    assert fabs_aff.frec is None
    assert fabs_aff.permission_type_id == PERMISSION_SHORT_DICT['f']

    # test creating DABS and FABS FREC permissions from three strings
    account_handler.set_caia_perms(user, ['FREC-ABCD-R', 'FREC-ABCD-S', 'FREC-ABCD-F'])
    database.session.commit()
    assert len(user.affiliations) == 3
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    frec_affiliations = list(sorted(frec_affils, key=lambda a: a.permission_type_id))
    dabs_aff, fabs_aff = frec_affiliations
    assert dabs_aff.cgac is None
    assert dabs_aff.frec.frec_code == 'ABCD'
    assert dabs_aff.permission_type_id == PERMISSION_TYPE_DICT['submitter']
    assert fabs_aff.cgac is None
    assert fabs_aff.frec.frec_code == 'ABCD'
    assert fabs_aff.permission_type_id == PERMISSION_SHORT_DICT['f']
    cgac_affils = [affil for affil in user.affiliations if affil.cgac is not None]
    assert cgac_affils[0].cgac.cgac_code == 'ABC'
    assert cgac_affils[0].frec is None
    assert cgac_affils[0].permission_type_id == PERMISSION_TYPE_DICT['reader']


@pytest.mark.usefixtures("user_constants")
def test_create_session_and_response(database, monkeypatch):
    cgacs = [CGACFactory(cgac_code=str(i) * 3, agency_name=str(i)) for i in range(3)]
    user = UserFactory(name="my name", title="my title", affiliations=[
        UserAffiliation(cgac=cgacs[1], permission_type_id=PERMISSION_TYPE_DICT['reader']),
        UserAffiliation(cgac=cgacs[2], permission_type_id=PERMISSION_TYPE_DICT['writer']),
    ])
    database.session.add_all(cgacs + [user])
    database.session.commit()

    monkeypatch.setattr(account_handler, 'LoginSession', Mock())

    mock_session = {'sid': 'test sid'}

    result = account_handler.AccountHandler.create_session_and_response(mock_session, user)
    result = json.loads(result.data.decode('utf-8'))
    assert result['message'] == 'Login successful'
    assert result['user_id'] == user.user_id
    assert result['name'] == 'my name'
    assert result['title'] == 'my title'
    assert result['session_id'] == 'test sid'
    assert dict(agency_name='1', permission='reader') in result['affiliations']
    assert dict(agency_name='2', permission='writer') in result['affiliations']
