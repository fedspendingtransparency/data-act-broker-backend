from random import randint
import os.path

import pytest

import dataactcore.config
# Load all models so we can access them through baseModel.Base.metadata
from dataactcore.models import (    # noqa
    baseModel, domainModels, fsrs, errorModels, jobModels, stagingModels,
    userModel, validationModels)
from dataactcore.scripts import setupJobTrackerDB, setupUserDB
from dataactcore.scripts.databaseSetup import (
    createDatabase, dropDatabase, runMigrations)
from dataactcore.interfaces.db import dbConnection


@pytest.fixture(scope='session')
def full_database_setup():
    """Sets up a clean database based on the model metadata. It also
    calculates the FK relationships between tables so we can delete them in
    order. It yields a tuple the _DB and ordered list of tables."""
    rand_id = str(randint(1, 9999))

    config = dataactcore.config.CONFIG_DB
    config['db_name'] = 'unittest{}_data_broker'.format(rand_id)
    dataactcore.config.CONFIG_DB = config

    createDatabase(config['db_name'])
    db = dbConnection()
    runMigrations()

    creation_order = baseModel.Base.metadata.sorted_tables
    yield (db, list(reversed(creation_order)))  # drop order

    db.close()
    dropDatabase(config['db_name'])


@pytest.fixture()
def database(full_database_setup):
    """Sets up a clean database if needed, deletes any models after each
    test"""
    db, tables_in_drop_order = full_database_setup
    yield db
    db.session.expire_all()
    for table in tables_in_drop_order:
        db.session.query(table).delete(synchronize_session=False)


@pytest.fixture()
def job_constants(database):
    setupJobTrackerDB.insertCodes(database.session)

@pytest.fixture()
def user_constants(database):
    setupUserDB.insertCodes(database.session)
    database.session.commit()

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
