import os
import logging
import pandas as pd
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import TASLookup
from dataactcore.interfaces.db import databaseSession
from dataactvalidator.scripts.loaderUtils import LoaderUtils


logger = logging.getLogger(__name__)


def loadTas(tasFile=None):
    """Load TAS file into broker database. """
    model = TASLookup

    # read TAS file to dataframe, to make sure all is well
    # with the file before firing up a db transaction
    if not tasFile:
        tasFile = os.path.join(
            CONFIG_BROKER["path"],
            "dataactvalidator",
            "config",
            "all_tas_betc.csv")
    data = pd.read_csv(tasFile, dtype=str,
                       # something in the file format skews the columns/names
                       # so grab the tas-related columns by index. not going
                       # to do a deep investigation here, since we're about
                       # to switch to a new source for the TAS file
                        usecols=[1, 2, 3, 4, 5, 6, 7])
    # we only need one row for each unique TAS
    data.drop_duplicates(inplace=True)

    with databaseSession() as sess:

        # delete existing data
        # TODO: when we swtich to loading TAS from CARS, do we sill want to delete existing recs?
        sess.query(model).delete()

        # clean TAS file
        data = LoaderUtils.cleanData(
            data,
            model,
            {"ata": "allocation_transfer_agency",
             "aid": "agency_identifier",
             "a": "availability_type_code",
             "bpoa": "beginning_period_of_availability",
             "epoa": "ending_period_of_availability",
             "main": "main_account_code",
             "sub": "sub_account_code",
             },
            {"allocation_transfer_agency": {"pad_to_length": 3, "keep_null": True},
                 "agency_identifier": {"pad_to_length": 3},
                 "main_account_code": {"pad_to_length": 4},
                 "sub_account_code": {"pad_to_length": 3},
            }
        )
        data = data.where((pd.notnull(data)), None)

        # instead of using the pandas to_sql dataframe method like
        # some of the other domain load processes, iterate through
        # the dataframe rows so we can load using the orm model
        # (note: toyed with the SQLAlchemy bulk load options but
        # ultimately decided not to go outside the unit of work
        # for the sake of a performance gain)
        for index, row in data.iterrows():
            sess.add(TASLookup(**row))

        sess.commit()
        logger.info('{} records inserted to {}'.format(
            len(data.index), model.__tablename__))
        return data

if __name__ == '__main__':
    loadTas()