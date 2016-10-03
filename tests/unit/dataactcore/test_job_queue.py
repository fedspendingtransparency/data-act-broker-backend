from collections import OrderedDict
import csv
import os
from unittest.mock import Mock

from dataactcore.utils import jobQueue


def read_file_rows(file_path):
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
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq1'))
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]
    jobQueue.generate_f_file(1, 1, Mock(), 'uniq1', 'uniq1', is_local=True)
    assert read_file_rows(file_path) == expected

    # re-order
    fileF_mock.mappings = OrderedDict(
        [('key11', 'mapping11'), ('key4', 'mapping4')])
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq2'))
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]
    jobQueue.generate_f_file(1, 1, Mock(), 'uniq2', 'uniq2', is_local=True)
    assert read_file_rows(file_path) == expected
