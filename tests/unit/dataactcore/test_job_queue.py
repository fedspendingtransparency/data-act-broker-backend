from dataactcore.utils import jobQueue
from dataactcore.config import CONFIG_BROKER
from unittest.mock import Mock
import pytest
from pytest import raises
from dataactcore.utils.responseException import ResponseException
import os


@pytest.fixture()
def mock_file_d_path(tmpdir):
    original = CONFIG_BROKER['d_file_storage_path']
    CONFIG_BROKER['d_file_storage_path'] = str(tmpdir) + os.path.sep
    yield tmpdir
    CONFIG_BROKER['d_file_storage_path'] = original


def test_generate_d_file_success(monkeypatch, mock_file_d_path):
    """ Test successful generation of D1 and D2 files """
    local_file_name = "12345_test_file.csv"
    file_path = mock_file_d_path.join(local_file_name)

    assert not os.path.isfile(str(file_path))

    file_path.write("test")

    result_xml = "<results>test_file.csv</results>"

    monkeypatch.setattr(jobQueue, 'download_file', Mock())
    monkeypatch.setattr(jobQueue, 'get_xml_response_content',
                        Mock(return_value=result_xml))
    jobQueue.generate_d_file(Mock(), 1, 1, Mock(), local_file_name, True)

    assert os.path.isfile(str(file_path))


def test_generate_d_file_failure(monkeypatch, mock_file_d_path):
    """ Test unsuccessful generation of D1 and D2 files """
    local_file_name = "12345_test_file.csv"
    file_path = mock_file_d_path.join(local_file_name)

    assert not os.path.isfile(str(file_path))

    monkeypatch.setattr(jobQueue, 'download_file', Mock())
    monkeypatch.setattr(jobQueue, 'get_xml_response_content',
                        Mock(return_value=''))
    with raises(ResponseException):
        jobQueue.generate_d_file(Mock(), 1, 1, Mock(), local_file_name, True)
