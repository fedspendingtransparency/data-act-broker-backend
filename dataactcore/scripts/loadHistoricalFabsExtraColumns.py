import os
import logging
import boto
import urllib.request
import zipfile
from collections import OrderedDict
import numpy as np
import pandas as pd

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data

logger = logging.getLogger(__name__)


def parse_fabs_file_new_columns(f, sess):
    csv_file = 'datafeeds\\' + os.path.splitext(os.path.basename(f.name))[0]

    # TODO update column header mappings (their column name: column number (0-indexed))
    # These are just placeholders so you have a sample
    column_header_mapping = {
        "awarding_sub_tier_agency": 0,
        "award_mod_num": 4,
        "federal_award_id": 7,
        "uri": 8,
        "funding_office_code": 9
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

    nrows = 0
    with zipfile.ZipFile(f.name) as zfile:
        with zfile.open(csv_file) as dat_file:
            nrows = len(dat_file.readlines())

    block_size = 10000
    batches = nrows // block_size
    last_block_size = (nrows % block_size)
    batch = 0
    added_rows = 0
    while batch <= batches:
        skiprows = 1 if batch == 0 else (batch * block_size)
        nrows = (((batch + 1) * block_size) - skiprows) if (batch < batches) else last_block_size
        logger.info('loading rows %s to %s', skiprows + 1, nrows + skiprows)

        with zipfile.ZipFile(f.name) as zip_file:
            with zip_file.open(csv_file) as dat_file:
                data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows,
                                   usecols=column_header_mapping_ordered.values(),
                                   names=column_header_mapping_ordered.keys())

                cdata = format_fabs_data(data)
                if cdata is not None:
                    for _, row in cdata.iterrows():
                        # TODO update this with the columns that need updating
                        sess.query(PublishedAwardFinancialAssistance).\
                            filter_by(afa_generated_unique=row['afa_generated_unique']).\
                            update({"funding_office_code": row['funding_office_code'],
                                    "funding_office_name": row['funding_office_name']},
                                   synchronize_session=False)

            added_rows += nrows
            batch += 1
            logger.info('%s PublishedAwardFinancialAssistance records updated', added_rows)
    sess.commit()


def format_fabs_data(data):
    if len(data.index) == 0:
        return None

    # TODO put all of the columns we need in here (right ours, left theirs)
    cdata = clean_data(
        data,
        PublishedAwardFinancialAssistance,
        {
            'agency_code': 'awarding_sub_tier_agency_c',
            'federal_award_mod': 'award_modification_amendme',
            'federal_award_id': 'fain',
            'uri': 'uri',
            'funding_office_code': 'funding_office_code'
        }, {}
    )

    # make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as
    # NULL in the db.
    cdata = cdata.replace(np.nan, '', regex=True)
    cdata = cdata.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    # generate the afa_generated_unique field
    cdata['afa_generated_unique'] = cdata.apply(lambda x: generate_unique_string(x), axis=1)

    # TODO might want to drop 'agency_code' and all the other stuff because we aren't updating them

    return cdata


def generate_unique_string(row):
    # TODO update these to be the correct headers from the file
    # create unique string from the awarding_sub_tier_agency_c, award_modification_amendme, fain, and uri
    astac = row['awarding_sub_tier_agency_c'] if row['awarding_sub_tier_agency_c'] is not None else '-none-'
    ama = row['award_modification_amendme'] if row['award_modification_amendme'] is not None else '-none-'
    fain = row['fain'] if row['fain'] is not None else '-none-'
    uri = row['uri'] if row['uri'] is not None else '-none-'
    return ama + "_" + astac + "_" + fain + "_" + uri


def main():
    sess = GlobalDB.db().session
    logger.info('Starting updates to FABS data')

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['archive_bucket'])
        # TODO change this to the correct file name
        new_columns_file = s3bucket.get_key("FIX FILE NAME HERE").generate_url(expires_in=600)
        parse_fabs_file_new_columns(urllib.request.urlopen(new_columns_file), sess)
    else:
        # TODO change this to the correct file name
        new_columns_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs", "FIX FILE NAME HERE")
        parse_fabs_file_new_columns(open(new_columns_file), sess)

    logger.info("Historical FABS column update script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()