from unittest.mock import Mock
import dataactbroker.handlers.accountHandler
import json
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


def test_max_login_success(database, user_constants, monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'CONFIG_BROKER', config)
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


def test_max_login_failure(monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock())
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'CONFIG_BROKER', config)

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have failed to login successfully with MAX"

    # Did not get a successful response from MAX
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = {
                    'cas:serviceResponse':
                    {
                        'cas:authenticationSuccess':
                        {
                            'cas:attributes':
                            {
                                'maxAttribute:Email-Address': '',
                                'maxAttribute:GroupList': ''
                            }
                        }
                     }
                }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have logged in with MAX but do not have permission to access the broker. " \
                    "Please contact DATABroker@fiscal.treasury.gov to obtain access."

    # Not in parent group
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = {
        'cas:serviceResponse':
            {
                'cas:authenticationSuccess':
                    {
                        'cas:attributes':
                            {
                                'maxAttribute:Email-Address': '',
                                'maxAttribute:GroupList': 'parent-group'
                            }
                    }
            }
    }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have logged in with MAX but do not have permission to access the broker. " \
                    "Please contact DATABroker@fiscal.treasury.gov to obtain access."

    # Not in cgac group
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']