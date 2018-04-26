from __future__ import with_statement
import logging
import os.path
import re
import sys

from alembic import context
from sqlalchemy import engine_from_config, pool

# Load all DB tables into metadata object
# @todo - load these dynamically
from dataactcore.models import (baseModel, domainModels, fsrs, errorModels, jobModels, stagingModels, # noqa
                                userModel, validationModels)
from dataactcore.config import CONFIG_DB
from dataactcore.interfaces.db import db_uri
from dataactcore.logging import configure_logging

USE_TWOPHASE = False

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Alembic recommends setting up logging here by clobbering existing
# configuration with its own. We generally have configured logging elsewhere,
# but we also want to set it up if running the alembic script
if 'alembic' in os.path.basename(sys.argv[0]):
    configure_logging()
logger = logging.getLogger('alembic.env')

# Use the broker's config file to gather section names referring to different
# databases. In db_dict, the key will = alembic .ini section names and
# migration method names. Value[0] will = the actual database name as
# set in the broker config. Value[1] is the corresponding model.
db_dict = {'data_broker': [CONFIG_DB['db_name'], baseModel]}
db_names = config.get_main_option('databases')
for name in re.split(r',\s*', db_names):
    if name not in db_dict:
        raise Exception('The alembic.ini databases section is targeting a database ({}) that is not set up in env.py. '
                        'Please add {} info to db_dict in env.py'.format(name, name))

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
# }
target_metadata = {key: value[1].Base.metadata for (key, value) in db_dict.items()}

# Set up database URLs based on config file
for (key, value) in db_dict.items():
    # key = db-related names expected by Alembic config/scripts
    # value[0] = actual db names as set in broker config file
    baseUrl = db_uri(value[0])
    config.set_section_option(key, 'sqlalchemy.url', baseUrl)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(schema_obj, schema_obj_name, type_, reflected, compare_to):
    """Use this function to ignore certain database object migrations when autogenerating
    schema_obj: A schemaItem object
    name: Name of object
    type_: Type of object (str) (ex: table, column, index)
    reflected: bool True if object was created based on table reflection, false if created based on MetaData object
    compare_to: schemaItem object the current object is being compared to (None if no object comparison)
    """

    # Skipping the following indexes since alembic tries to drop these indexes
    # These fields are created and updated by the TimeStampMixin
    if type_ == 'index' and schema_obj_name in ['ix_detached_award_procurement_updated_at',
                                                'ix_published_award_financial_assistance_created_at']:
        logger.info("Skipping schema migration for object {}".format(schema_obj_name))
        return False

    return True


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
    for db_name in re.split(r',\s*', db_names):
        engines[db_name] = rec = {}
        rec['url'] = context.config.get_section_option(db_name, "sqlalchemy.url")

    for db_name, rec in engines.items():
        logger.info("Migrating database %s" % db_name)
        file_ = "%s.sql" % db_name
        logger.info("Writing output to %s" % file_)
        with open(file_, 'w') as buffer:
            context.configure(url=rec['url'], output_buffer=buffer,
                              target_metadata=target_metadata.get(db_name),
                              literal_binds=True,
                              include_object=include_object)
            with context.begin_transaction():
                context.run_migrations(engine_name=db_name)


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # for the direct-to-DB use case, start a transaction on all
    # engines, then run all migrations, then commit all transactions.

    engines = {}
    for db_name in re.split(r',\s*', db_names):
        engines[db_name] = rec = {}
        rec['engine'] = engine_from_config(
            context.config.get_section(db_name),
            prefix='sqlalchemy.',
            poolclass=pool.NullPool)

    for db_name, rec in engines.items():
        engine = rec['engine']
        rec['connection'] = conn = engine.connect()

        if USE_TWOPHASE:
            rec['transaction'] = conn.begin_twophase()
        else:
            rec['transaction'] = conn.begin()

    try:
        for db_name, rec in engines.items():
            logger.info("Migrating database %s" % db_name)
            context.configure(
                connection=rec['connection'],
                upgrade_token="%s_upgrades" % db_name,
                downgrade_token="%s_downgrades" % db_name,
                target_metadata=target_metadata.get(db_name),
                compare_type=True,  # instruct autogen to detect col type changes
                include_object=include_object
            )
            context.run_migrations(engine_name=db_name)

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
