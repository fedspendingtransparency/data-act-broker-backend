import os
import logging

import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import TASLookup
from dataactvalidator.app import createApp
from dataactvalidator.scripts.loaderUtils import LoaderUtils


logger = logging.getLogger(__name__)


def cleanTas(csvPath):
    """Read a CSV into a dataframe, then use a configured `cleanData` and
    return the results"""
    data = pd.read_csv(csvPath, dtype=str)
    data = LoaderUtils.cleanData(
        data,
        TASLookup,
        {"a": "availability_type_code",
         "acct_num": "tas_id",
         "aid": "agency_identifier",
         "ata": "allocation_transfer_agency",
         "bpoa": "beginning_period_of_availability",
         "epoa": "ending_period_of_availability",
         "main": "main_account_code",
         "sub": "sub_account_code",
         },
        {"allocation_transfer_agency": {"pad_to_length": 3, "keep_null": True},
         "agency_identifier": {"pad_to_length": 3},
         # Account for " " cells
         "availability_type_code": {"pad_to_length": 0, "keep_null": True},
         "beginning_period_of_availability": {"pad_to_length": 0,
                                              "keep_null": True},
         "ending_period_of_availability": {"pad_to_length": 0,
                                           "keep_null": True},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3},
         }
    )
    data["tas_id"] = pd.to_numeric(data["tas_id"])
    return data.where(pd.notnull(data), None)


def updateTASLookups(csvPath):
    """Load TAS data from the provided CSV and replace/insert any
    TASLookups"""
    sess = GlobalDB.db().session

    data = cleanTas(csvPath)
    # Delete all existing TAS records -- we don't want to accept submissions
    # after the entries fall off the CARS file
    sess.query(TASLookup).delete(synchronize_session=False)

    # instead of using the pandas to_sql dataframe method like some of the
    # other domain load processes, iterate through the dataframe rows so we
    # can load using the orm model (note: toyed with the SQLAlchemy bulk load
    # options but ultimately decided not to go outside the unit of work for
    # the sake of a performance gain)
    for _, row in data.iterrows():
        sess.add(TASLookup(**row))

    sess.commit()
    logger.info('%s records inserted to %s', len(data.index),
                TASLookup.__tablename__)


def loadTas(tasFile=None):
    """Load TAS file into broker database. """
    # read TAS file to dataframe, to make sure all is well
    # with the file before firing up a db transaction
    if not tasFile:
        tasFile = os.path.join(
            CONFIG_BROKER["path"],
            "dataactvalidator",
            "config",
            "cars_tas.csv")

    with createApp().app_context():
        updateTASLookups(tasFile)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loadTas()
