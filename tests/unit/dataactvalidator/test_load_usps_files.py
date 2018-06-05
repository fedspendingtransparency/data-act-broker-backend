import urllib.parse
import pytest
import json
import re
from unittest.mock import MagicMock
from dataactvalidator.scripts.load_usps_files import get_payload_string, check_response_status, get_login_tokens, \
    get_file_info


def mocked_requests_post(url):
    """ Mocks post request to USPS service. Returns a MagicMock in the structure of a request object. """

    if url == 'http://working_url.com':
        return MagicMock(status_code=200, text=json.dumps({'key': 'value', 'response': 'success'}))

    return MagicMock(status_code=500, text=json.dumps({'error': 'message', 'messages': 'failure'}))


def test_get_pay_load_string():
    """ Tests get_payload_string() to ensure payload is properly formatted"""
    test_dict = {"a": "123", "b": "456"}
    result = get_payload_string(test_dict)
    assert re.match(r'^obj={\"[a-b]\":\"\d{3}\",$\n\"[a-b]\":\"\d{3}\"}', urllib.parse.unquote(result), re.M)


def test_check_response_status_success():
    """ Tests check_response_status() function when a POST request is successful (status 200) """
    test_response = check_response_status(mocked_requests_post('http://working_url.com'))
    assert test_response == {'key': 'value', 'response': 'success'}


def test_check_response_failed():
    """ Tests check_response_status() function when a POST request is a failure (status 500) """
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


def test_get_file_info(database):
    """ Tests the get_file_info() function: returns the fileid for the latest version of the zip4 file to be downloaded
    """
    test_data = {"response": "success", "fileList": [{"fileid": "12345", "status": "N", "filepath": "file/path/",
                                                      "filename": "anotherfile.tar", "fulfilled": "2012-08-15"},
                                                     {"fileid": "23456", "status": "N", "filepath": "file/path/",
                                                     "filename": "somefile.tar", "fulfilled": "2012-10-15"},
                                                     {"fileid": "23456", "status": "N", "filepath": "file/path/",
                                                     "filename": "laterfile.tar", "fulfilled": "2012-09-15"}]}

    sess = database.session
    assert get_file_info(sess, test_data) == ('23456', '2012-10-15', None)
