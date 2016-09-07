from collections import OrderedDict
import csv
import os
from unittest.mock import Mock

from pytest import raises

from dataactcore.utils import jobQueue
from dataactcore.utils.responseException import ResponseException


def test_generate_d_file_success(monkeypatch, mock_broker_config_paths):
    """ Test successful generation of D1 and D2 files """
    local_file_name = "12345_test_file.csv"
    file_path = mock_broker_config_paths['d_file_storage_path'].join(
        local_file_name)

    assert not os.path.isfile(str(file_path))

    file_path.write("test")

    result_xml = "<results>test_file.csv</results>"

    monkeypatch.setattr(jobQueue, 'download_file', Mock())
    monkeypatch.setattr(jobQueue, 'get_xml_response_content',
                        Mock(return_value=result_xml))
    jobQueue.generate_d_file(Mock(), 1, 1, Mock(), local_file_name, True)

    assert os.path.isfile(str(file_path))


def test_generate_d_file_failure(monkeypatch, mock_broker_config_paths):
    """ Test unsuccessful generation of D1 and D2 files """
    local_file_name = "12345_test_file.csv"
    file_path = mock_broker_config_paths['d_file_storage_path'].join(
        local_file_name)

    assert not os.path.isfile(str(file_path))

    monkeypatch.setattr(jobQueue, 'download_file', Mock())
    monkeypatch.setattr(jobQueue, 'get_xml_response_content',
                        Mock(return_value=''))
    with raises(ResponseException):
        jobQueue.generate_d_file(Mock(), 1, 1, Mock(), local_file_name, True)


def read_f_file_rows(suffix, file_path):
    jobQueue.generate_f_file(1, 1, 1, Mock(), suffix, is_local=True)

    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]


def test_generate_f_file(monkeypatch, mock_broker_config_paths):
    """A CSV with fields in the right order should be written to the file
    system"""
    fileF_mock = Mock()
    monkeypatch.setattr(jobQueue, 'fileF', fileF_mock)
    fileF_mock.generateFRows.return_value = [
        dict(key4='a', key11='b'), dict(key4='c', key11='d')
    ]

    fileF_mock.mappings = OrderedDict(
        [('key4', 'mapping4'), ('key11', 'mapping11')])
    file_path = str(mock_broker_config_paths['broker_files'].join('unique1'))
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]
    assert read_f_file_rows('unique1', file_path) == expected

    # re-order
    fileF_mock.mappings = OrderedDict(
        [('key11', 'mapping11'), ('key4', 'mapping4')])
    file_path = str(mock_broker_config_paths['broker_files'].join('unique2'))
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]
    assert read_f_file_rows('unique2', file_path) == expected
