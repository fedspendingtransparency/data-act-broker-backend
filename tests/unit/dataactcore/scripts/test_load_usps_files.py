import pytest
import json
from datetime import datetime

from unittest.mock import Mock

from dataactcore.models.domainModels import ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactcore.scripts.pipeline import load_usps_files
from tests.unit.mock_helpers import MockSession


def mocked_requests_post(url):
    """Mocks post request to USPS service. Returns a MagicMock in the structure of a request object."""

    if url == "http://working_url.com":
        return Mock(status_code=200, text=None)

    return Mock(status_code=500, text=json.dumps({"error": "message"}))


def test_check_response_status_success():
    """Tests check_response_status() function when a POST request is successful (status 200)"""
    test_response = load_usps_files.check_response_status(mocked_requests_post("http://working_url.com"))
    assert test_response == {}


def test_check_response_failed():
    """Tests check_response_status() function when a POST request is a failure (status 500)"""
    with pytest.raises(SystemExit) as pytest_wrapped_error:
        load_usps_files.check_response_status(mocked_requests_post("http://not_working_url.com"))

    assert pytest_wrapped_error.type == SystemExit
    assert pytest_wrapped_error.value.code == 1


def test_get_login_token(monkeypatch):
    """Tests the get_login_token() function: returns a token to use for subsequent calls"""
    monkeypatch.setattr(load_usps_files, "usps_epf_request", Mock(return_value={"token": "abc123"}))
    assert load_usps_files.get_login_token() == "abc123"


def test_get_file_info():
    """Tests the get_file_info() function: returns the fileid for the latest version of the zip4 file to be
    downloaded"""

    # Create test external data load date
    last_load_date = datetime.strptime("1700-01-01", "%Y-%m-%d")
    test_external_data_load_date = ExternalDataLoadDate(
        external_data_load_date_id=-1,
        last_load_date_start=last_load_date,
        last_load_date_end=last_load_date,
        external_data_type_id=EXTERNAL_DATA_TYPE_DICT["usps_download"],
    )

    mock_session = MockSession()
    mock_session.query("").filter_by()._first = test_external_data_load_date

    test_data = [
        {
            "fileId": "12345",
            "status": "N",
            "fileName": "anotherfile.tar",
            "fulfilledDate": "2012-08-15",
            "fileSize": 12345,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
        {
            "fileId": "23456",
            "status": "N",
            "fileName": "somefile.tar",
            "fulfilledDate": "2012-10-15",
            "fileSize": 67890,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
        {
            "fileId": "23456",
            "status": "N",
            "fileName": "laterfile.tar",
            "fulfilledDate": "2012-09-15",
            "fileSize": 13579,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
    ]

    expected_date = datetime.strptime("2012-10-15", "%Y-%m-%d").date()
    assert load_usps_files.get_file_info(mock_session, test_data) == (
        "23456",
        expected_date,
        test_external_data_load_date,
    )


def _assert_system_exit(expected_code, f, *args):
    with pytest.raises(SystemExit) as cm:
        f(*args)
        if isinstance(cm.exception, int):
            assert cm.exception == expected_code
        else:
            assert cm.exception.code == expected_code


def test_exit_code_3():
    # Create test external data load date
    last_load_date = datetime.strptime("2013-01-01", "%Y-%m-%d")
    test_external_data_load_date = ExternalDataLoadDate(
        external_data_load_date_id=-1,
        last_load_date_start=last_load_date,
        last_load_date_end=last_load_date,
        external_data_type_id=EXTERNAL_DATA_TYPE_DICT["usps_download"],
    )

    mock_session = MockSession()
    mock_session.query("").filter_by()._first = test_external_data_load_date

    test_data = [
        {
            "fileId": "12345",
            "status": "N",
            "fileName": "anotherfile.tar",
            "fulfilledDate": "2012-08-15",
            "fileSize": 12345,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
        {
            "fileId": "23456",
            "status": "N",
            "fileName": "somefile.tar",
            "fulfilledDate": "2012-10-15",
            "fileSize": 12345,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
        {
            "fileId": "23456",
            "status": "N",
            "fileName": "laterfile.tar",
            "fulfilledDate": "2012-09-15",
            "fileSize": 12345,
            "account": None,
            "productCode": "ABC",
            "productId": "ABC123",
        },
    ]

    _assert_system_exit(3, load_usps_files.get_file_info, mock_session, test_data)
