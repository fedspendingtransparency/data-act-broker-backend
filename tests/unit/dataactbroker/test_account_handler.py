import json
from unittest.mock import Mock

from dataactbroker.handlers import accountHandler
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from tests.unit.dataactcore.factories.user import UserFactory


def test_max_login_success(database, user_constants, monkeypatch):
    ah = accountHandler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(accountHandler, 'CONFIG_BROKER', config)
    max_dict = {
        'cas:serviceResponse':
            {
                'cas:authenticationSuccess':
                    {
                        'cas:attributes':
                            {
                                'maxAttribute:Email-Address': 'test-user@email.com',
                                'maxAttribute:GroupList': 'parent-group,parent-group-CGAC_SYS',
                                'maxAttribute:First-Name': 'test',
                                'maxAttribute:Middle-Name': '',
                                'maxAttribute:Last-Name': 'user'
                            }
                    }
            }
    }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = {
        'cas:serviceResponse':
            {
                'cas:authenticationSuccess':
                    {
                        'cas:attributes':
                            {
                                'maxAttribute:Email-Address': 'test-user-1@email.com',
                                'maxAttribute:GroupList': '',
                                'maxAttribute:First-Name': 'test-1',
                                'maxAttribute:Middle-Name': '',
                                'maxAttribute:Last-Name': 'user-1'
                            }
                    }
            }
    }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_max_login_failure(monkeypatch):
    ah = accountHandler.AccountHandler(Mock())
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(accountHandler, 'CONFIG_BROKER', config)

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have failed to login successfully with MAX"

    # Did not get a successful response from MAX
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_grant_highest_permission():
    """Verify that we get the _highest_ permission within our CGAC"""
    user = UserFactory()
    group_list = ['prefix-ABC-PERM_R', 'prefix-ABC-PERM_S']
    accountHandler.grant_highest_permission(user, group_list, ['prefix-ABC'])
    assert user.cgac_code == 'ABC'
    assert user.permission_type_id == PERMISSION_TYPE_DICT['submitter']

    group_list = ['prefix-ABC-PERM_W']
    accountHandler.grant_highest_permission(user, group_list, ['prefix-ABC'])
    assert user.cgac_code == 'ABC'
    assert user.permission_type_id == PERMISSION_TYPE_DICT['writer']
