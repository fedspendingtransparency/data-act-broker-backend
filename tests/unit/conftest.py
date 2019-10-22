from random import randint
import os.path

from flask import Flask, g
import pytest

import dataactcore.config
from dataactcore.models import baseModel
from dataactcore.scripts import setup_job_tracker_db, setup_user_db, setup_validation_db
from dataactcore.scripts.database_setup import create_database, drop_database, run_migrations
from dataactcore.interfaces.db import GlobalDB


@pytest.fixture(scope='session')
def full_database_setup():
    """Sets up a clean database based on the model metadata. It also
    calculates the FK relationships between tables so we can delete them in
    order. It yields a tuple the _DB and ordered list of tables."""
    rand_id = str(randint(1, 9999))

    config = dataactcore.config.CONFIG_DB
    config['db_name'] = 'unittest{}_data_broker'.format(rand_id)
    dataactcore.config.CONFIG_DB = config

    create_database(config['db_name'])
    db = GlobalDB.db()
    run_migrations()

    creation_order = baseModel.Base.metadata.sorted_tables
    yield (db, list(reversed(creation_order)))  # drop order

    GlobalDB.close()
    drop_database(config['db_name'])


@pytest.fixture()
def database(full_database_setup):
    """Sets up a clean database if needed, deletes any models after each
    test"""
    db, tables_in_drop_order = full_database_setup
    yield db
    db.session.expire_all()
    # rollback all open transactions
    db.scoped_session_maker.rollback()
    for table in tables_in_drop_order:
        db.session.query(table).delete(synchronize_session=False)


@pytest.fixture()
def job_constants(database):
    setup_job_tracker_db.insert_codes(database.session)


@pytest.fixture()
def validation_constants(database):
    setup_validation_db.insert_codes(database.session)


@pytest.fixture()
def user_constants(database):
    setup_user_db.insert_codes(database.session)
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


@pytest.fixture
def broker_files_tmp_dir():
    """Make sure this directory exists, as some tests write files to it"""
    cfg = dataactcore.config.CONFIG_BROKER
    if cfg['local'] and not os.path.exists(cfg['broker_files']):
        os.makedirs(cfg['broker_files'])


@pytest.fixture
def test_app(database):
    """Gets us in the application context, where we can set/use g.
    Particularly useful if we're checking `g` in multiple modules (and hence
    Mocking them out would be a pain)"""
    app = Flask('test-app')
    with app.app_context():
        g._db = database
        yield app.test_client()
