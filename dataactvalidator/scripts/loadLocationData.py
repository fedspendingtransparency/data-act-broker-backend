import os
import logging
import boto
import pandas as pd
from datetime import datetime
import urllib.request

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import CityCode, CountyCode, States, ZipCity

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
    num = insert_dataframe(data, CityCode.__table__.name, sess.connection())
    logger.info('{} records inserted to city_code'.format(num))
    sess.commit()


def parse_county_file(county_file, sess):
    # read the data and clean up the column names
    data = pd.read_csv(county_file, dtype=str, sep="|")
    data = clean_data(
        data,
        {"COUNTY_NUMERIC": "county_number",
         "COUNTY_NAME": "county_name",
         "STATE_ALPHA": "state_code"})

    # remove all blank county_number rows. Not much use in a county number table
    data = data[pd.notnull(data['county_number'])]

    # remove duplicates because we have no use for them (there may be none, this is a precaution)
    data = data[~data.duplicated(subset=['county_number', 'state_code'], keep='first')]

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    # insert data into table
    num = insert_dataframe(data, CountyCode.__table__.name, sess.connection())
    logger.info('{} records inserted to county_code'.format(num))
    sess.commit()


def parse_state_file(state_file, sess):
    # read the data. Cleaning is in there in case something changes, doesn't really do anything now
    data = pd.read_csv(state_file, dtype=str)
    data = clean_data(
        data,
        {"state_name": "state_name",
         "state_code": "state_code"})

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    # insert data into table
    num = insert_dataframe(data, States.__table__.name, sess.connection())
    logger.info('{} records inserted to states'.format(num))
    sess.commit()


def parse_zip_city_file(f, sess):
    line_size = 129
    chunk_size = 1024 * 10
    f.read(line_size)

    data_array = {}
    curr_chunk = ""

    while True:
        # grab the next chunk
        next_chunk = f.read(chunk_size)
        # when streaming from S3 it reads in as bytes, we need to decode it as a utf-8 string
        if not type(next_chunk) == str:
            next_chunk = next_chunk.decode("utf-8")

        # add the new chunk of the file to the current chunk we're processing
        curr_chunk += next_chunk

        # if the current chunk is smaller than the line size, we're done
        if len(curr_chunk) < line_size:
            break

        # while we can still do more processing on the current chunk, process it per line
        while len(curr_chunk) >= line_size:
            # grab another line and get the data if it's a "detail record"
            curr_row = curr_chunk[:line_size]
            if curr_row[0] == "D":
                zip_code = curr_row[1:6]
                city_name = curr_row[62:90]
                data_array[zip_code] = {"zip_code": zip_code, "city_name": city_name}

            # cut the current line out of the chunk we're processing
            curr_chunk = curr_chunk[line_size:]

    sess.bulk_save_objects([ZipCity(**zip_data) for _, zip_data in data_array.items()])
    logger.info('{} records inserted to zip_city'.format(len(data_array)))
    sess.commit()


def load_city_data(city_file):
    with create_app().app_context():
        sess = GlobalDB.db().session

        # delete any data in the CityCode table
        sess.query(CityCode).delete()

        # parse the new city code data
        parse_city_file(city_file, sess)


def load_county_data(county_file):
    with create_app().app_context():
        sess = GlobalDB.db().session

        # delete any data in the CityCode table
        sess.query(CountyCode).delete()

        # parse the new county code data
        parse_county_file(county_file, sess)


def load_state_data(state_file):
    with create_app().app_context():
        sess = GlobalDB.db().session

        # delete any data in the States table
        sess.query(States).delete()

        # parse the new state data
        parse_state_file(state_file, sess)


def load_zip_city_data(zip_city_file):
    with create_app().app_context():
        sess = GlobalDB.db().session

        # delete any data in the ZipCity table
        sess.query(ZipCity).delete()

        # parse the new zip city data
        parse_zip_city_file(zip_city_file, sess)


def load_location_data():
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        city_file = s3bucket.get_key("NationalFedCodes.txt").generate_url(expires_in=600)
        county_file = s3bucket.get_key("GOVT_UNITS.txt").generate_url(expires_in=600)
        state_file = s3bucket.get_key("state_list.txt").generate_url(expires_in=600)
        citystate_file = s3bucket.get_key("ctystate.txt").generate_url(expires_in=600)
        zip_city_file = urllib.request.urlopen(citystate_file)
    else:
        city_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "NationalFedCodes.txt")
        county_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "GOVT_UNITS.txt")
        state_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "state_list.txt")
        citystate_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "ctystate.txt")
        zip_city_file = open(citystate_file)

    logger.info('Loading city data')
    load_city_data(city_file)
    logger.info('Loading county data')
    load_county_data(county_file)
    logger.info('Loading state data')
    load_state_data(state_file)
    logger.info('Loading zip city data')
    load_zip_city_data(zip_city_file)


if __name__ == '__main__':
    configure_logging()
    load_location_data()
