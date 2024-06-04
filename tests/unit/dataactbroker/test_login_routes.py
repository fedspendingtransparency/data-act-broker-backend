import json
from unittest.mock import patch
from sqlalchemy import func

from tests.unit.dataactbroker.test_account_handler import make_caia_user_dict, make_caia_token_dict
import dataactbroker.handlers.account_handler as account_handler
from dataactcore.models.userModel import User, UserAffiliation
from dataactcore.interfaces.db import GlobalDB

CAIA_RESPONSE_NO_PERMS = make_caia_user_dict("[]")
CAIA_RESPONSE_W_PERMS = make_caia_user_dict("CGAC-999-R")
CAIA_TOKEN_DICT = make_caia_token_dict('123456789')

EXAMPLE_API_PROXY_TOKEN = 'test-token'

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

LOGIN_RESPONSE = {
    "user_id": 1,
    "name": "Test User",
    "title": "Test User",
    "skip_guide": False,
    "website_admin": False,
    "affiliations": [],
    "session_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "message": "Login successful"
}


@patch('dataactbroker.handlers.account_handler.get_max_dict')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_no_perms_broker_user_max(create_session_mock, max_dict_mock, database, monkeypatch):
    ah = max_login_func(create_session_mock, max_dict_mock, monkeypatch, MAX_RESPONSE_NO_PERMS)
    res = ah.max_login({})
    response = json.loads(res.get_data().decode("utf-8"))
    sess = GlobalDB.db().session
    # This is to prevent an integrity error with other tests that create users.
    sess.query(User).filter(func.lower(User.email) == func.lower("something@test.com"))\
        .delete(synchronize_session=False)
    sess.commit()
    assert response['message'] == "There are no Data Broker permissions assigned to this Service Account. You " \
                                  "may request permissions at https://community.max.gov/x/fJwuRQ"


@patch('dataactbroker.handlers.account_handler.get_caia_user_dict')
@patch('dataactbroker.handlers.account_handler.get_caia_tokens')
@patch('dataactbroker.handlers.account_handler.revoke_caia_access')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_no_perms_broker_user_caia(create_session_mock, revoke_caia_mock, caia_token_mock, caia_dict_mock, database,
                                   monkeypatch):
    ah = caia_login_func(create_session_mock, revoke_caia_mock, caia_token_mock, caia_dict_mock, monkeypatch,
                         CAIA_RESPONSE_NO_PERMS)
    res = ah.caia_login({})
    sess = GlobalDB.db().session
    # This is to prevent an integrity error with other tests that create users.
    test_user = sess.query(User).filter(func.lower(User.email) == func.lower("test-user@email.com"))
    affiliations = sess.query(UserAffiliation).filter_by(user_id=test_user.one().user_id).all()
    test_user.delete(synchronize_session=False)
    sess.commit()
    assert res is True
    assert affiliations == []


@patch('dataactbroker.handlers.account_handler.get_max_dict')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_w_perms_broker_user_max(create_session_mock, max_dict_mock, database, monkeypatch):
    ah = max_login_func(create_session_mock, max_dict_mock, monkeypatch, MAX_RESPONSE_W_PERMS)
    res = ah.max_login({})
    sess = GlobalDB.db().session
    # This is to prevent an integrity error with other tests that create users.
    sess.query(User).filter(func.lower(User.email) == func.lower("something@test.com"))\
        .delete(synchronize_session=False)
    sess.commit()
    assert res is True


@patch('dataactbroker.handlers.account_handler.get_caia_user_dict')
@patch('dataactbroker.handlers.account_handler.get_caia_tokens')
@patch('dataactbroker.handlers.account_handler.revoke_caia_access')
@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_w_perms_broker_user_caia(create_session_mock, revoke_caia_mock, caia_token_mock, caia_dict_mock, database,
                                  monkeypatch):
    ah = caia_login_func(create_session_mock, revoke_caia_mock, caia_token_mock, caia_dict_mock, monkeypatch,
                         CAIA_RESPONSE_W_PERMS)
    res = ah.caia_login({})
    sess = GlobalDB.db().session
    # This is to prevent an integrity error with other tests that create users.
    sess.query(User).filter(func.lower(User.email) == func.lower("test-user@email.com"))\
        .delete(synchronize_session=False)
    sess.commit()
    assert res is True


@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_proxy_login_success(create_session_mock, monkeypatch, database):
    sess = GlobalDB.db().session
    test_user = User(email='test-user@email.com')
    sess.add(test_user)

    ah = proxy_login_func(create_session_mock, monkeypatch, EXAMPLE_API_PROXY_TOKEN)
    res = ah.proxy_login({})

    # This is to prevent an integrity error with other tests that create users.
    sess.query(User).filter(func.lower(User.email) == func.lower("test-user@email.com"))\
        .delete(synchronize_session=False)

    assert res.get('session_id') is not None


@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_proxy_login_invalid_user(create_session_mock, monkeypatch, database):
    ah = proxy_login_func(create_session_mock, monkeypatch, EXAMPLE_API_PROXY_TOKEN)
    res = ah.proxy_login({})
    response = json.loads(res.get_data().decode("utf-8"))

    sess = GlobalDB.db().session
    # This is to prevent an integrity error with other tests that create users.
    sess.query(User).filter(func.lower(User.email) == func.lower("test-user@email.com"))\
        .delete(synchronize_session=False)

    assert response['message'] == "Invalid user"


@patch('dataactbroker.handlers.account_handler.AccountHandler.create_session_and_response')
def test_proxy_login_invalid_token(create_session_mock, monkeypatch):
    ah = proxy_login_func(create_session_mock, monkeypatch, 'different token')
    res = ah.proxy_login({})
    response = json.loads(res.get_data().decode("utf-8"))

    assert response['message'] == "Invalid token"


def max_login_func(create_session_mock, max_dict_mock, monkeypatch, max_response):
    def json_return():
        return {"ticket": "12345", "service": "https://some.url.gov"}
    request = type('Request', (object,), {"is_json": True, "headers": {"Content-Type": "application/json"},
                                          "get_json": json_return})
    ah = account_handler.AccountHandler(request=request)
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', {"parent_group": "test"})
    max_dict_mock.return_value = max_response
    create_session_mock.return_value = True
    return ah


def caia_login_func(create_session_mock, revoke_caia_mock, caia_token_mock, caia_dict_mock, monkeypatch, caia_response):
    def json_return():
        return {"code": "12345", "redirect_uri": "https://some.url.gov"}
    request = type('Request', (object,), {"is_json": True, "headers": {"Content-Type": "application/json"},
                                          "get_json": json_return})
    ah = account_handler.AccountHandler(request=request)
    caia_dict_mock.return_value = caia_response
    caia_token_mock.return_value = CAIA_TOKEN_DICT
    revoke_caia_mock.return_value = None
    create_session_mock.return_value = True
    return ah


def proxy_login_func(create_session_mock, monkeypatch, token):
    def json_return():
        return {"name": "test-user@email.com", "token": token}
    request = type('Request', (object,), {"is_json": True, "headers": {"Content-Type": "application/json"},
                                          "get_json": json_return})
    ah = account_handler.AccountHandler(request=request)
    monkeypatch.setattr(account_handler, 'CONFIG_BROKER', {"api_proxy_token": EXAMPLE_API_PROXY_TOKEN})
    create_session_mock.return_value = LOGIN_RESPONSE
    return ah
