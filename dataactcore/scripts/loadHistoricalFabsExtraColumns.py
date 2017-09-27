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

    column_header_mapping = {"agency_code": 0, "federal_award_mod": 1, "federal_award_id": 2, "uri": 3,
                             "awarding office code": 4, "awarding office name": 5, "funding office name": 6,
                             "funding office code": 7, "funding agency name": 8, "funding agency code": 9,
                             "funding sub tier agency code": 10, "funding sub tier agency name": 11,
                             "legal entity foreign city": 12, "legal entity foreign province": 13,
                             "legal entity foreign postal code": 14, "legal entity foreign location description": 15}
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

    nrows = 0
    with zipfile.ZipFile(f.name) as zfile:
        with zfile.open(csv_file) as dat_file:
            nrows = len(dat_file.readlines())

    block_size, batch, added_rows = 10000, 0, 0
    batches = nrows // block_size
    last_block_size = (nrows % block_size)
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
                        sess.query(PublishedAwardFinancialAssistance).\
                            filter_by(afa_generated_unique=row['afa_generated_unique']).\
                            update({"awarding_office_code": row['awarding_office_code'],
                                    "awarding_office_name": row['awarding_office_name'],
                                    "funding_office_name": row['funding_office_name'],
                                    "funding_office_code": row['funding_office_code'],
                                    "funding_agency_name": row['funding_agency_name'],
                                    "funding_agency_code": row['funding_agency_code'],
                                    "funding_sub_tier_agency_co": row['funding_sub_tier_agency_co'],
                                    "funding_sub_tier_agency_na": row['funding_sub_tier_agency_na'],
                                    "legal_entity_foreign_city": row['legal_entity_foreign_city'],
                                    "legal_entity_foreign_provi": row['legal_entity_foreign_provi'],
                                    "legal_entity_foreign_posta": row['legal_entity_foreign_posta'],
                                    "legal_entity_foreign_descr": row['legal_entity_foreign_descr']},
                                   synchronize_session=False)

            added_rows += nrows
            batch += 1
            logger.info('%s PublishedAwardFinancialAssistance records updated', added_rows)
    sess.commit()


def format_fabs_data(data):
    # drop all records without any data to be loaded
    data = data.replace('', np.nan, inplace=True)
    data.dropna(subset=["awarding office code", "awarding office name", "funding office name", "funding office code",
                        "funding agency name", "funding agency code", "funding sub tier agency code",
                        "funding sub tier agency name", "legal entity foreign city", "legal entity foreign province",
                        "legal entity foreign postal code", "legal entity foreign location description"], inplace=True)

    # ensure there are rows to be cleaned and formatted
    if len(data.index) == 0:
        return None

    cdata = clean_data(
        data,
        PublishedAwardFinancialAssistance,
        {
            "agency_code": "awarding_sub_tier_agency_c",
            "federal_award_mod": "award_modification_amendme",
            "federal_award_id": "fain",
            "uri": "uri",
            "awarding office code": "awarding_office_code",
            "awarding office name": "awarding_office_name",
            "funding office name": "funding_office_name",
            "funding office code": "funding_office_code",
            "funding agency name": "funding_agency_name",
            "funding agency code": "funding_agency_code",
            "funding sub tier agency code": "funding_sub_tier_agency_co",
            "funding sub tier agency name": "funding_sub_tier_agency_na",
            "legal entity foreign city": "legal_entity_foreign_city",
            "legal entity foreign province": "legal_entity_foreign_provi",
            "legal entity foreign postal code": "legal_entity_foreign_posta",
            "legal entity foreign location description": "legal_entity_foreign_descr"
        }, {}
    )

    # make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as
    # NULL in the db.
    cdata = cdata.replace(np.nan, '', regex=True)
    cdata = cdata.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    # generate the afa_generated_unique field
    cdata['afa_generated_unique'] = cdata.apply(lambda x: generate_unique_string(x), axis=1)

    # drop columns in afa_generated_unique because we aren't updating them
    for col in ["awarding_sub_tier_agency_c", "award_modification_amendme", "fain", "uri"]:
        del cdata[col]

    return cdata


def generate_unique_string(row):
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
        new_columns_file = s3bucket.get_key("Assistance_DataActFields_2017.csv").generate_url(expires_in=600)
        parse_fabs_file_new_columns(urllib.request.urlopen(new_columns_file), sess)
    else:
        new_columns_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs",
                                        "Assistance_DataActFields_2017.csv")
        parse_fabs_file_new_columns(open(new_columns_file), sess)

    logger.info("Historical FABS column update script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
