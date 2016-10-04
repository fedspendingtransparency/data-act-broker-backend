from random import randint
import os.path

import pytest

import dataactcore.config
# Load all models so we can access them through baseModel.Base.__subclasses__
from dataactcore.models import (    # noqa
    baseModel, domainModels, fsrs, errorModels, jobModels, stagingModels,
    userModel, validationModels)
from dataactcore.scripts.databaseSetup import (
    createDatabase, dropDatabase, runMigrations)
from dataactcore.interfaces.db import dbConnection


@pytest.fixture(scope='session')
def full_database_setup():
    """Sets up a clean database, yielding a relevant interface holder"""
    rand_id = str(randint(1, 9999))

    config = dataactcore.config.CONFIG_DB
    config['db_name'] = 'unittest{}_data_broker'.format(rand_id)
    dataactcore.config.CONFIG_DB = config

    createDatabase(config['db_name'])
    runMigrations()
    db = dbConnection()

    yield db

    db.close()
    dropDatabase(config['db_name'])


@pytest.fixture()
def database(full_database_setup):
    """Sets up a clean database if needed, deletes any models after each
    test"""
    yield full_database_setup
    sess = full_database_setup.session

    for model in baseModel.Base.__subclasses__():
        sess.query(model).delete(synchronize_session=False)
    sess.expire_all()


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
