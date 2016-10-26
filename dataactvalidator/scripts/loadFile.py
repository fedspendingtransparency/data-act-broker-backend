import os
import logging

import pandas as pd

from dataactvalidator.app import createApp
from dataactvalidator.scripts.loaderUtils import LoaderUtils
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, ObjectClass, ProgramActivity
from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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

    logger.info('Loading CGAC')
    loadCgac(os.path.join(basePath,"cgac.csv"))
    logger.info('Loading object class')
    loadObjectClass(os.path.join(basePath,"object_class.csv"))
    logger.info('Loading program activity')

    if localProgramActivity is not None:
        loadProgramActivity(localProgramActivity)
    else:
        loadProgramActivity(os.path.join(basePath, "program_activity.csv"))


if __name__ == '__main__':
    loadDomainValues(
        os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    )
