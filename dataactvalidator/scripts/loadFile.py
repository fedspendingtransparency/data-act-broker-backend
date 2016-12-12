import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CGAC, ObjectClass, ProgramActivity
from dataactvalidator.app import createApp
from dataactvalidator.scripts.loaderUtils import LoaderUtils

logger = logging.getLogger(__name__)


def loadCgac(filename):
    """Load CGAC (high-level agency names) lookup table."""
    model = CGAC

    with createApp().app_context():
        sess = GlobalDB.db().session

        # for CGAC, delete and replace values
        sess.query(model).delete()

        # read CGAC values from csv
        data = pd.read_csv(filename, dtype=str)
        # clean data
        data = LoaderUtils.cleanData(
            data,
            model,
            {"cgac": "cgac_code", "agency": "agency_name"},
            {"cgac_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['cgac_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadObjectClass(filename):
    """Load object class lookup table."""
    model = ObjectClass

    with createApp().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = LoaderUtils.cleanData(
            data,
            model,
            {"max_oc_code": "object_class_code",
             "max_object_class_name": "object_class_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadProgramActivity(filename):
    """Load program activity lookup table."""
    model = ProgramActivity

    with createApp().app_context():
        sess = GlobalDB.db().session

        # for program activity, delete and replace values??
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = LoaderUtils.cleanData(
            data,
            model,
            {"year": "budget_year",
             "agency_id": "agency_id",
             "alloc_id": "allocation_transfer_id",
             "account": "account_number",
             "pa_code": "program_activity_code",
             "pa_name": "program_activity_name"},
            {"program_activity_code": {"pad_to_length": 4},
             "agency_id": {"pad_to_length": 3},
             "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
             "account_number": {"pad_to_length": 4}
             }
        )
        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load.
        data.drop_duplicates(inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadDomainValues(basePath, localProgramActivity = None):
    """Load all domain value files.

    Parameters
    ----------
        basePath : directory that contains the domain values files.
        localProgramActivity : optional location of the program activity file (None = use basePath)
    """
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        cgac_file = s3bucket.get_key("cgac.csv")
        object_class_file = s3bucket.get_key("object_class.csv")
        program_activity_file = s3bucket.get_key("program_activity.csv")

    else:
        cgac_file = os.path.join(basePath,"cgac.csv")
        object_class_file = os.path.join(basePath,"object_class.csv")
        program_activity_file = os.path.join(basePath, "program_activity.csv") 

    logger.info('Loading CGAC')
    loadCgac(cgac_file)
    logger.info('Loading object class')
    loadObjectClass(object_class_file)
    logger.info('Loading program activity')

    if localProgramActivity is not None:
        loadProgramActivity(localProgramActivity)
    else:
        loadProgramActivity(program_activity_file)


if __name__ == '__main__':
    configure_logging()
    loadDomainValues(
        os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    )
