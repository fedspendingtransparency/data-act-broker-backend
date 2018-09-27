import json
import pytest
from unittest.mock import Mock

from dataactbroker.handlers import account_handler
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PERMISSION_SHORT_DICT
from dataactcore.models.userModel import UserAffiliation
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from tests.unit.mock_helpers import mock_response
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.user import UserFactory


def make_max_dict(group_str):
    """We need to create mock MAX data at multiple points in these tests"""
    return {
        'cas:serviceResponse': {
            'cas:authenticationSuccess': {
                'cas:attributes': {
                    'maxAttribute:Email-Address': 'test-user@email.com',
                    'maxAttribute:GroupList': group_str,
                    'maxAttribute:First-Name': 'test',
                    'maxAttribute:Middle-Name': '',
                    'maxAttribute:Last-Name': 'user',
                    'maxAttribute:MAX-ID': 'id'
                }
            }
        }
    }


@pytest.mark.usefixtures("user_constants")
def test_max_login_success_normal_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = False
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    max_dict = {'cas:serviceResponse': {}}
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', config)
    max_dict = make_max_dict('parent-group,parent-group-CGAC_SYS')
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = make_max_dict('')
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_max_login_failure_normal_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', config)

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = False
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    max_dict = {'cas:serviceResponse': {}}
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = ("The Max CAS endpoint was unable to locate your session using "
                     "the ticket/service combination you provided.")

    # Did not get a successful response from MAX
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']


@pytest.mark.usefixtures("user_constants")
def test_max_login_success_cert_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = True
    mock_dict.return_value.safeDictionary.side_effect = {'cert': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    max_dict = {'cas:serviceResponse': {}}
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))

    config = {'parent_group': 'parent-group', 'full_url': 'full-url', 'max_cert_url': 'max-cert-url'}
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', config)

    max_dict = make_max_dict('parent-group,parent-group-CGAC_SYS')
    max_dict['cas:serviceResponse']['cas:authenticationSuccess']['cas:attributes']['maxAttribute:MAX-ID'] = 'S_id'
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))

    mock_resp = Mock()
    mock_resp.return_value = mock_response(url='ticket=12345')
    monkeypatch.setattr('requests.get', mock_resp)

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = make_max_dict('')
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_max_login_failure_cert_login(monkeypatch):
    ah = account_handler.AccountHandler(Mock())
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', config)

    mock_dict = Mock()
    mock_dict.return_value.exists.return_value = True
    mock_dict.return_value.safeDictionary.side_effect = {'cert': ''}
    monkeypatch.setattr(account_handler, 'RequestDictionary', mock_dict)

    mock_resp = Mock()
    mock_resp.return_value = mock_response(url='ticket=12345')
    monkeypatch.setattr('requests.get', mock_resp)

    config = {'full_url': 'full-url', 'max_cert_url': 'max-cert-url'}
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', config)

    max_dict = {'cas:serviceResponse': {}}
    monkeypatch.setattr(account_handler, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = ("The Max CAS endpoint was unable to locate your session using "
                     "the ticket/service combination you provided.")

    # Did not get a successful response from MAX
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_set_user_name_updated():
    """ Tests set_user_name()  updates a user's name """

    user = UserFactory(name="No User")

    mock_cas_attrs = {
                    'maxAttribute:First-Name': 'New',
                    'maxAttribute:Middle-Name': '',
                    'maxAttribute:Last-Name': 'Name'
                    }

    account_handler.set_user_name(user, mock_cas_attrs)

    assert user.name == 'New Name'


def test_set_user_name_middle_name():
    """ Tests set_user_name() adds a middle name initial to the user's name when a middle name is provided """

    user = UserFactory()

    mock_cas_attrs = {
                    'maxAttribute:First-Name': 'Test',
                    'maxAttribute:Middle-Name': 'Abc',
                    'maxAttribute:Last-Name': 'User'
                    }

    account_handler.set_user_name(user, mock_cas_attrs)

    assert user.name == 'Test A. User'


def test_set_user_name_empty_middle_name():
    """ Tests set_user_name() omits a middle name initial to the user's name when a middle name is empty (spaces) """
    user = UserFactory()

    mock_cas_attrs = {
                    'maxAttribute:First-Name': 'Test',
                    'maxAttribute:Middle-Name': ' ',
                    'maxAttribute:Last-Name': 'User'
                }

    account_handler.set_user_name(user, mock_cas_attrs)

    assert user.name == 'Test User'


def test_set_user_name_no_middle_name():
    """ Tests set_user_name() omits a middle name initial to the user's name when a middle name is empty (None) """

    user = UserFactory()

    mock_cas_attrs = {
                    'maxAttribute:First-Name': 'Test',
                    'maxAttribute:Middle-Name': None,
                    'maxAttribute:Last-Name': 'User'
                }

    account_handler.set_user_name(user, mock_cas_attrs)

    assert user.name == 'Test User'


@pytest.mark.usefixtures("user_constants")
def test_set_max_perms(database, monkeypatch):
    """Verify that we get the _highest_ permission within our CGAC"""
    cgac_abc = CGACFactory(cgac_code='ABC')
    cgac_def = CGACFactory(cgac_code='DEF')
    frec_abc = FRECFactory(frec_code='ABCD', cgac=cgac_abc)
    frec_def = FRECFactory(frec_code='EFGH', cgac=cgac_abc)
    user = UserFactory()
    database.session.add_all([cgac_abc, cgac_def, frec_abc, frec_def, user])
    database.session.commit()

    monkeypatch.setitem(account_handler.CONFIG_BROKER, 'parent_group', 'prefix')

    # test creating permission from string
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-PERM_W')
    database.session.commit()   # populate ids
    assert len(user.affiliations) == 1
    affil = user.affiliations[0]
    assert affil.cgac_id == cgac_abc.cgac_id
    assert affil.permission_type_id == PERMISSION_TYPE_DICT['writer']

    # test creating max CGAC permission from two strings
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-PERM_R,prefix-CGAC_ABC-PERM_S')
    database.session.commit()   # populate ids
    assert len(user.affiliations) == 1
    affil = user.affiliations[0]
    assert affil.cgac_id == cgac_abc.cgac_id
    assert affil.permission_type_id == PERMISSION_TYPE_DICT['submitter']

    # test creating two CGAC permissions with two strings
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-PERM_R,prefix-CGAC_DEF-PERM_S')
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

    # test creating max FREC permission from two strings
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-FREC_ABCD-PERM_R,prefix-CGAC_ABC-FREC_ABCD-PERM_S')
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
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-FREC_ABCD-PERM_R,prefix-CGAC_ABC-FREC_EFGH-PERM_S')
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
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-PERM_S,prefix-CGAC_DEF-FREC_ABCD-PERM_R')
    database.session.commit()
    assert len(user.affiliations) == 3
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    assert frec_affils[0].cgac is None
    assert frec_affils[0].frec.frec_code == 'ABCD'
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

    # test creating max DABS and FABS CGAC permissions from three strings
    account_handler.set_max_perms(user, 'prefix-CGAC_ABC-PERM_R,prefix-CGAC_ABC-PERM_S,prefix-CGAC_ABC-PERM_F')
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

    # test creating max DABS and FABS FREC permissions from three strings
    perms_string = 'prefix-CGAC_ABC-FREC_ABCD-PERM_R,prefix-CGAC_ABC-FREC_ABCD-PERM_S,prefix-CGAC_ABC-FREC_ABCD-PERM_F'
    account_handler.set_max_perms(user, perms_string)
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

    # test creating DABS and FABS CGAC permissions from service accounts
    perms_string = 'prefix-CGAC_ABC-PERM_W'
    account_handler.set_max_perms(user, perms_string, service_account_flag=True)
    database.session.commit()
    assert len(user.affiliations) == 2
    abc_aff, def_aff = list(sorted(user.affiliations, key=lambda a: a.permission_type_id))
    assert abc_aff.cgac.cgac_code == 'ABC'
    assert abc_aff.frec is None
    assert abc_aff.permission_type_id == PERMISSION_TYPE_DICT['writer']
    assert def_aff.cgac.cgac_code == 'ABC'
    assert def_aff.frec is None
    assert def_aff.permission_type_id == PERMISSION_SHORT_DICT['e']

    # test creating DABS and FABS FREC permissions from service accounts
    perms_string = 'prefix-CGAC_ABC-FREC_ABCD-PERM_F'
    account_handler.set_max_perms(user, perms_string, service_account_flag=True)
    database.session.commit()
    assert len(user.affiliations) == 3
    frec_affils = [affil for affil in user.affiliations if affil.frec is not None]
    frec_affiliations = list(sorted(frec_affils, key=lambda a: a.permission_type_id))
    dabs_aff, fabs_aff = frec_affiliations
    assert dabs_aff.cgac is None
    assert dabs_aff.frec.frec_code == 'ABCD'
    assert dabs_aff.permission_type_id == PERMISSION_TYPE_DICT['writer']
    assert fabs_aff.cgac is None
    assert fabs_aff.frec.frec_code == 'ABCD'
    assert fabs_aff.permission_type_id == PERMISSION_SHORT_DICT['e']
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
