import json
from unittest.mock import patch


import dataactbroker.handlers.account_handler as account_handler
from dataactcore.models.userModel import User
from dataactcore.interfaces.db import GlobalDB
from sqlalchemy import func

MAX_RESPONSE_NO_PERMS = {
    "cas:serviceResponse": {
        "cas:authenticationSuccess": {
            "cas:attributes": {
                'maxAttribute:MAX-ID': "s_something",
                "maxAttribute:Email-Address": "something@test.com",
                'maxAttribute:GroupList': None,
                'maxAttribute:First-Name': "Bob",
                'maxAttribute:Last-Name': "Jones",
                'maxAttribute:Middle-Name': None
            }
        }
    }
}

MAX_RESPONSE_W_PERMS = {
    "cas:serviceResponse": {
        "cas:authenticationSuccess": {
            "cas:attributes": {
                'maxAttribute:MAX-ID': "s_something",
                "maxAttribute:Email-Address": "something@test.com",
                'maxAttribute:GroupList': "test_CGAC_hello",
                'maxAttribute:First-Name': "Bob",
                'maxAttribute:Last-Name': "Jones",
                'maxAttribute:Middle-Name': None
            }
        }
    }
}


@patch('dataactbroker.handlers.account_handler.get_max_dict')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_no_perms_broker_user(create_session_mock, max_dict_mock, database, monkeypatch):
    ah = max_login_func(create_session_mock, max_dict_mock, monkeypatch, MAX_RESPONSE_NO_PERMS)
    res = ah.max_login({})
    response = json.loads(res.get_data().decode("utf-8"))
    sess = GlobalDB.db().session
    user = sess.query(User).filter(func.lower(User.email) == func.lower("something@test.com")).delete()
    sess.commit()
    assert response['message'] == "There are no permissions assigned to this user!"


@patch('dataactbroker.handlers.account_handler.get_max_dict')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_w_perms_broker_user(create_session_mock, max_dict_mock, database, monkeypatch):
    ah = max_login_func(create_session_mock, max_dict_mock, monkeypatch, MAX_RESPONSE_W_PERMS)
    res = ah.max_login({})
    sess = GlobalDB.db().session
    user = sess.query(User).filter(func.lower(User.email) == func.lower("something@test.com")).delete()
    sess.commit()               
    assert res is True



def max_login_func(create_session_mock, max_dict_mock, monkeypatch, max_response):
    def json_return(): return {"ticket": "12345", "service": "https://some.url.gov"}
    request = type('Request', (object,),  {"is_json": True, "headers": {"Content-Type": "application/json"},
                                           "get_json": json_return})
    ah = account_handler.AccountHandler(request=request)
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', {"parent_group": "test"})
    max_dict_mock.return_value = max_response
    create_session_mock.return_value = True
    return ah
