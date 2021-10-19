import os
import sys
import logging
import boto3
import pandas as pd
from datetime import datetime
import urllib.request

from dataactbroker.helpers.pandas_helper import check_dataframe_diff
from dataactbroker.helpers.uri_helper import RetrieveFileFromUri

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import CityCode, CountyCode, States, ZipCity

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import insert_dataframe, trim_item

logger = logging.getLogger(__name__)


def clean_data(data, field_map):
    """ Clean up the data by removing columns that aren't relevant and renaming the remaining ones to match what we
        need.

        Args:
            data: data to clean
            field_map: mapping of all relevant columns
    """
    # toss out any columns from the csv that aren't in the fieldMap parameter
    data = data[list(field_map.keys())]

    # rename columns as specified in fieldMap
    data = data.rename(columns=field_map)

    # trim all columns
    data = data.applymap(lambda x: trim_item(x) if len(str(x).strip()) else None)
    return data


def parse_city_file(city_file):
    """ Parse the City file and insert all relevant rows into the database.

        Args:
            city_file: path/url to file to gather City data from

        Returns:
            The data in a pandas object, cleaned and sorted for insertion into the DB
    """
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

    return data


def parse_county_file(county_file):
    """ Parse the County file and insert all relevant rows into the database.

        Args:
            county_file: path/url to file to gather County data from

        Returns:
            The data in a pandas object, cleaned and sorted for insertion into the DB
    """
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

    return data


def parse_state_file(state_file):
    """ Parse the State file and insert all relevant rows into the database.

        Args:
            state_file: path/url to file to gather State data from

        Returns:
            The data in a pandas object, cleaned and sorted for insertion into the DB
    """
    # read the data. Cleaning is in there in case something changes, doesn't really do anything now
    data = pd.read_csv(state_file, dtype=str)
    data = clean_data(
        data,
        {"state_name": "state_name",
         "state_code": "state_code",
         "fips_code": "fips_code"})

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    return data


def parse_zip_city_file(f):
    """ Parse the ZipCity file and insert all relevant rows into the database.

        Args:
            f: file to process

        Returns:
            The data in a pandas object, cleaned and sorted for insertion into the DB
    """
    line_size = 129
    chunk_size = 1024 * 10
    f.read(line_size)

    data_dict = {}
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
                city_name = curr_row[62:90].strip()
                data_dict[zip_code] = {"zip_code": zip_code, "city_name": city_name}

            # cut the current line out of the chunk we're processing
            curr_chunk = curr_chunk[line_size:]

    data = pd.DataFrame([[item['zip_code'], item['city_name']] for _, item in data_dict.items()],
                        columns=['zip_code', 'city_name'])

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    return data


def load_city_data(force_reload):
    """ Load data into the CityCode table

        Args:
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    # parse the new city code data
    city_file_url = '{}/NationalFedCodes.txt'.format(CONFIG_BROKER['usas_public_reference_url'])
    with RetrieveFileFromUri(city_file_url, 'r').get_file_object() as city_file:
        new_data = parse_city_file(city_file)

    diff_found = check_dataframe_diff(new_data, CityCode, ['city_code_id'], ['state_code', 'city_code'])

    if force_reload or diff_found:
        sess = GlobalDB.db().session
        logger.info('Differences found or reload forced, reloading city_code table.')
        # delete any data in the CityCode table
        sess.query(CityCode).delete()

        # insert data into table
        num = insert_dataframe(new_data, CityCode.__table__.name, sess.connection())
        logger.info('{} records inserted to city_code'.format(num))
        sess.commit()
    else:
        logger.info('No differences found, skipping city_code table reload.')


def load_county_data(force_reload):
    """ Load data into the CountyCode table

        Args:
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    county_file_url = '{}/GOVT_UNITS.txt'.format(CONFIG_BROKER['usas_public_reference_url'])
    with RetrieveFileFromUri(county_file_url, 'r').get_file_object() as county_file:
        new_data = parse_county_file(county_file)

    diff_found = check_dataframe_diff(new_data, CountyCode, ['county_code_id'], ['county_number', 'state_code'])

    if force_reload or diff_found:
        sess = GlobalDB.db().session
        logger.info('Differences found or reload forced, reloading county_code table.')
        # delete any data in the CountyCode table
        sess.query(CountyCode).delete()

        # insert data into table
        num = insert_dataframe(new_data, CountyCode.__table__.name, sess.connection())
        logger.info('{} records inserted to county_code'.format(num))
        sess.commit()
    else:
        logger.info('No differences found, skipping county_code table reload.')


def load_state_data(force_reload):
    """ Load data into the States table

        Args:
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    state_file_url = '{}/state_list.csv'.format(CONFIG_BROKER['usas_public_reference_url'])
    with RetrieveFileFromUri(state_file_url, 'r').get_file_object() as state_file:
        new_data = parse_state_file(state_file)

    diff_found = check_dataframe_diff(new_data, States, ['states_id'], ['state_code'])

    if force_reload or diff_found:
        sess = GlobalDB.db().session
        logger.info('Differences found or reload forced, reloading states table.')
        # delete any data in the States table
        sess.query(States).delete()

        # insert data into table
        num = insert_dataframe(new_data, States.__table__.name, sess.connection())
        logger.info('{} records inserted to states'.format(num))
        sess.commit()
    else:
        logger.info('No differences found, skipping states table reload.')


def load_zip_city_data(force_reload):
    """ Load data into the ZipCity table

        Args:
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    if CONFIG_BROKER["use_aws"]:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        citystate_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                         'Key': "ctystate.txt"}, ExpiresIn=600)
        zip_city_file = urllib.request.urlopen(citystate_file)
    else:
        citystate_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "ctystate.txt")
        zip_city_file = open(citystate_file)

    new_data = parse_zip_city_file(zip_city_file)

    diff_found = check_dataframe_diff(new_data, ZipCity, ['zip_city_id'], ['zip_code'])

    if force_reload or diff_found:
        sess = GlobalDB.db().session
        logger.info('Differences found or reload forced, reloading zip_city table.')
        # delete any data in the ZipCity table
        sess.query(ZipCity).delete()

        # insert data into table
        num = insert_dataframe(new_data, ZipCity.__table__.name, sess.connection())
        logger.info('{} records inserted to zip_city'.format(num))
        sess.commit()
    else:
        logger.info('No differences found, skipping zip_city table reload.')


def load_location_data(force_reload=False):
    """ Loads the city, county, state, citystate, and zipcity data.

        Args:
            force_reload: reloads the tables even if there are no differences found in data
    """
    with create_app().app_context():
        logger.info('Loading city data')
        load_city_data(force_reload)
        logger.info('Loading county data')
        load_county_data(force_reload)
        logger.info('Loading state data')
        load_state_data(force_reload)
        logger.info('Loading zip city data')
        load_zip_city_data(force_reload)


if __name__ == '__main__':
    configure_logging()
    reload = '--force' in sys.argv
    load_location_data(reload)
