from random import randint

import pytest

import dataactcore.config
from dataactcore.scripts.databaseSetup import (
    createDatabase, dropDatabase, runMigrations)
from dataactcore.interfaces.db import dbConnection


@pytest.fixture(scope='session')
def database():
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
