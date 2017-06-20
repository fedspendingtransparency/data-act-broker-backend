import os
import logging
import boto
import pandas as pd
from datetime import datetime

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import CityCode

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import insert_dataframe

logger = logging.getLogger(__name__)


def clean_data(data, field_map):
    # toss out any columns from the csv that aren't in the fieldMap parameter
    data = data[list(field_map.keys())]

    # rename columns as specified in fieldMap
    data = data.rename(columns=field_map)
    return data


def parse_city_file(city_file, sess):
    logger.info("Beginning data parsing")
    # read the data and clean up the column names
    data = pd.read_csv(city_file, dtype=str, sep="|")
    data = clean_data(
        data,
        {"FEATURE_NAME": "feature_name",
         "FEATURE_CLASS": "feature_class",
         "CENSUS_CODE": "city_code",
         "STATE_ALPHA": "state_code",
         "COUNTY_NUMERIC": "county_number",
         "COUNTY_NAME": "county_name",
         "PRIMARY_LATITUDE": "latitude",
         "PRIMARY_LONGITUDE": "longitude"})

    # add a sort column based on feature_class and remove anything with a different feature class or empty city_code
    feature_class_ranking = {"Populated Place": 1, "Locale": 2, "Civil": 3, "Census": 4}
    data = data[pd.notnull(data['city_code'])]
    data['sorting_col'] = data['feature_class'].map(feature_class_ranking)
    data = data[pd.notnull(data['sorting_col'])]

    # sort by feature_class then remove any duplicates within state/city code combo (we keep the first occurrence
    # because we've sorted by priority so the one that would overwrite the others is on top already)
    data = data.sort_values(by=['sorting_col'])
    data = data[~data.duplicated(subset=['state_code', 'city_code'], keep='first')]
    data = data.drop('sorting_col', axis=1)

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    # just sorting it how it started out
    data = data.sort_values(by=['feature_name'])

    # insert data into table
    logger.info("Data parsing complete, inserting into CityCode table")
    insert_dataframe(data, CityCode.__table__.name, sess.connection())
    sess.commit()


def load_city_values(sess):
    # delete any data in the CityCode table
    logger.info('Deleting CityCode data')
    sess.query(CityCode).delete(synchronize_session=False)
    sess.commit()

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        city_file = s3bucket.get_key("NationalFedCodes.txt").generate_url(expires_in=600)
    else:
        city_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "NationalFedCodes.txt")

    parse_city_file(city_file, sess)

    logger.info("City Code script complete")


def main():
    sess = GlobalDB.db().session
    load_city_values(sess)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
