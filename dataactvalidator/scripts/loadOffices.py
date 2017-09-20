from collections import defaultdict
import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import FPDSContractingOffice
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data


logger = logging.getLogger(__name__)


def clean_office(csv_path):
    """Read a CSV into a dataframe, then use a configured `clean_data` and
    return the results"""
    data = pd.read_csv(csv_path, dtype=str)
    data = clean_data(
        data,
        FPDSContractingOffice,
        {"department_id": "department_id",
         "department_name": "department_name",
         "agency_code": "agency_code",
         "agency_name": "agency_name",
         "contracting_office_code": "contracting_office_code",
         "contracting_office_name": "contracting_office_name",
         "start_date": "start_date",
         "end_date": "end_date",
         "address_city": "address_city",
         "address_state": "address_state",
         "zip_code": "zip_code",
         "country_code": "country_code"
         },
        {"department_id": {"pad_to_length": 4}}
    )
    return data.where(pd.notnull(data), None)


def update_offices(csv_path):
    """Load office data from the provided CSV and replace/insert any
    office lookups"""
    sess = GlobalDB.db().session

    data = clean_office(csv_path)
    add_existing_id(data)

    old_data = data[data['existing_id'].notnull()]
    del old_data['existing_id']

    new_data = data[data['existing_id'].isnull()]
    del new_data['existing_id']

    # instead of using the pandas to_sql dataframe method like some of the
    # other domain load processes, iterate through the dataframe rows so we
    # can load using the orm model (note: toyed with the SQLAlchemy bulk load
    # options but ultimately decided not to go outside the unit of work for
    # the sake of a performance gain)
    for _, row in old_data.iterrows():
        sess.query(FPDSContractingOffice).filter_by(contracting_office_code=row['contracting_office_code'])\
            .update(row, synchronize_session=False)

    for _, row in new_data.iterrows():
        sess.add(FPDSContractingOffice(**row))

    sess.commit()
    logger.info('%s records in CSV, %s existing',
                len(data.index), sum(data['existing_id'].notnull()))


def load_offices(load_office=None):
    """Load TAS file into broker database. """
    # read office file to dataframe, to make sure all is well
    # with the file before firing up a db transaction
    if not load_office:
        if CONFIG_BROKER["use_aws"]:
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
            load_office = s3bucket.get_key("FPDSNG_Contracting_Offices.csv").generate_url(expires_in=600)
        else:
            load_office = os.path.join(
                CONFIG_BROKER["path"],
                "dataactvalidator",
                "config",
                "FPDSNG_Contracting_Offices.csv")

    with create_app().app_context():
        update_offices(load_office)


def add_existing_id(data):
    """Look up the ids of existing TASes. Use contracting_office_code as a non-unique
    identifier to help filter results"""
    existing = defaultdict(list)
    sess = GlobalDB.db().session
    query = sess.query(FPDSContractingOffice).filter(FPDSContractingOffice.contracting_office_code
                                                     .in_([i for i in data['contracting_office_code']]))
    for tas in query:
        existing[tas.contracting_office_code].append(tas)

    data['existing_id'] = data.apply(existing_id, axis=1, existing=existing)


def existing_id(row, existing):
    """ Check for a TASLookup which matches this `row` in the `existing` data.
        Args:
            row: row to check in
            existing: Dict[account_num, List[TASLookup]]
    """
    for potential_match in existing[row['contracting_office_code']]:
        return potential_match.contracting_office_code


if __name__ == '__main__':
    configure_logging()
    load_offices()
