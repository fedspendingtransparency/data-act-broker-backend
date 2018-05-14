import urllib.parse
import pytest
import json
from unittest.mock import MagicMock
from dataactvalidator.scripts.load_usps_files import get_payload_string, check_response_status, get_login_tokens, \
    get_file_info


def mocked_requests_post(url):

    if url == 'http://working_url.com':
        return MagicMock(status_code=200, text=json.dumps({'key': 'value', 'response': 'success'}))

    return MagicMock(status_code=500, text=json.dumps({'error': 'message', 'messages': 'failure'}))


def test_get_pay_load_screen():

    test_dict = {"a": "123", "b": "456"}
    result = get_payload_string(test_dict)
    assert urllib.parse.unquote(result) == 'obj={"a":"123",\n"b":"456"}'


def test_check_response_status_success():
    test_response = check_response_status(mocked_requests_post('http://working_url.com'))
    assert test_response == {'key': 'value', 'response': 'success'}


def test_check_response_failed():
    """ Tests check reponse() function: Creates dictionary of login keys to use to make additional
            request calls to USPS
    """
    with pytest.raises(SystemExit) as pytest_wrapped_error:
        check_response_status(mocked_requests_post('http://not_working_url.com'))

    assert pytest_wrapped_error.type == SystemExit
    assert pytest_wrapped_error.value.code == 1


def test_get_login_tokens():
    """ Tests the get_login_tokens() function: Creates dictionary of login keys to use to make additional
        request calls to USPS
    """
    test_data = {'logonkey': '1234', 'tokenkey': 'abc123', 'message': 'Success'}
    assert get_login_tokens(test_data) == {'logonkey': '1234', 'tokenkey': 'abc123'}


def test_get_file_info():
    """ Tests the get_file_info() function: returns the fileid for the latest version of the zip4 file to be downloaded
    """
    test_data = {"response": "success", "fileList": [{"fileid": "12345", "status": "N", "filepath": "file/path/",
                                                      "filename": "anotherfile.tar", "fulfilled": "2012-08-15"},
                                                     {"fileid": "23456", "status": "N", "filepath": "file/path/",
                                                     "filename": "somefile.tar", "fulfilled": "2012-10-15"},
                                                     {"fileid": "23456", "status": "N", "filepath": "file/path/",
                                                     "filename": "laterfile.tar", "fulfilled": "2012-09-15"}]}

    assert get_file_info(test_data) == '23456'
