from __future__ import with_statement
from alembic import context
# Load all DB tables into metadata object
# @todo - load these dynamically
from dataactcore.models import baseModel, domainModels, fsrs, errorModels, jobModels, stagingModels, userModel, validationModels # noqa
from dataactcore.config import CONFIG_DB
from dataactcore.interfaces.db import dbURI
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig
import logging
import re

USE_TWOPHASE = False

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')

# Use the broker's config file to gather section names referring to different
# databases. In db_dict, the key will = alembic .ini section names and
# migration method names. Value[0] will = the actual database name as
# set in the broker config. Value[1] is the corresponding model.
db_dict = {}
db_dict['data_broker'] = [CONFIG_DB['db_name'], baseModel]
db_names = config.get_main_option('databases')
for name in re.split(r',\s*', db_names):
    if name not in db_dict:
        raise Exception('The alembic.ini databases section is targeting '
                        'a database ({}) that is not set up in env.py. '
                        'Please add {} info to db_dict in env.py'.
                        format(name, name))

# add your model's MetaData objects here
# for 'autogenerate' support.  These must be set
# up to hold just those tables targeting a
# particular database. table.tometadata() may be
# helpful here in case a "copy" of
# a MetaData is needed.
# from myapp import mymodel
# target_metadata = {
#       'engine1':mymodel.metadata1,
#       'engine2':mymodel.metadata2
#}
target_metadata = {key: value[1].Base.metadata for (key, value) in db_dict.items()}

# Set up database URLs based on config file
for (key, value) in db_dict.items():
    # key = db-related names expected by Alembic config/scripts
    # value[0] = actual db names as set in broker config file
    baseUrl = dbURI(value[0])
    config.set_section_option(key, 'sqlalchemy.url', baseUrl)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # for the --sql use case, run migrations for each URL into
    # individual files.

    engines = {}
    for name in re.split(r',\s*', db_names):
        engines[name] = rec = {}
        rec['url'] = context.config.get_section_option(name,
                                                       "sqlalchemy.url")

    for name, rec in engines.items():
        logger.info("Migrating database %s" % name)
        file_ = "%s.sql" % name
        logger.info("Writing output to %s" % file_)
        with open(file_, 'w') as buffer:
            context.configure(url=rec['url'], output_buffer=buffer,
                              target_metadata=target_metadata.get(name),
                              literal_binds=True)
            with context.begin_transaction():
                context.run_migrations(engine_name=name)


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # for the direct-to-DB use case, start a transaction on all
    # engines, then run all migrations, then commit all transactions.

    engines = {}
    for name in re.split(r',\s*', db_names):
        engines[name] = rec = {}
        rec['engine'] = engine_from_config(
            context.config.get_section(name),
            prefix='sqlalchemy.',
            poolclass=pool.NullPool)

    for name, rec in engines.items():
        engine = rec['engine']
        rec['connection'] = conn = engine.connect()

        if USE_TWOPHASE:
            rec['transaction'] = conn.begin_twophase()
        else:
            rec['transaction'] = conn.begin()

    try:
        for name, rec in engines.items():
            logger.info("Migrating database %s" % name)
            context.configure(
                connection=rec['connection'],
                upgrade_token="%s_upgrades" % name,
                downgrade_token="%s_downgrades" % name,
                target_metadata=target_metadata.get(name),
                compare_type=True # instruct autogen to detect col type changes
            )
            context.run_migrations(engine_name=name)

        if USE_TWOPHASE:
            for rec in engines.values():
                rec['transaction'].prepare()

        for rec in engines.values():
            rec['transaction'].commit()
    except:
        for rec in engines.values():
            rec['transaction'].rollback()
        raise
    finally:
        for rec in engines.values():
            rec['connection'].close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
