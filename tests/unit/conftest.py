from random import randint
import os

import pytest

import dataactcore.config
from dataactcore.scripts.databaseSetup import (
    createDatabase, dropDatabase, runMigrations)
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder


@pytest.fixture(scope='session')
def database():
    """Sets up a clean database, yielding a relevant interface holder"""
    rand_id = str(randint(1, 9999))

    config = dataactcore.config.CONFIG_DB
    config['db_name'] = 'unittest{}_data_broker'.format(rand_id)
    dataactcore.config.CONFIG_DB = config

    createDatabase(config['db_name'])
    runMigrations()
    interface = InterfaceHolder()

    yield interface

    interface.close()
    dropDatabase(config['db_name'])


@pytest.fixture()
def mock_broker_config_paths(tmpdir):
    """Replace configured paths with temp directories which will be cleaned up
    at the end of testing."""
    # Expand as needed
    keys_to_replace = {'d_file_storage_path', 'broker_files'}
    original = dict(dataactcore.config.CONFIG_BROKER)   # shallow copy

    paths = {}
    for key in keys_to_replace:
        tmp_path = tmpdir.mkdir(key)
        paths[key] = tmp_path
        dataactcore.config.CONFIG_BROKER[key] = str(tmp_path) + os.path.sep

    yield paths

    for key in keys_to_replace:
        dataactcore.config.CONFIG_BROKER[key] = original[key]
