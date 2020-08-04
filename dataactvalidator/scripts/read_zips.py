# Local tests took 1 hour to insert 5 million lines. Estimated 9 hours to insert all data as of 05/2017.
# Suggested to run on a weekend during off hours

import os
import re
import logging
import boto3
import urllib.request
import pandas as pd

from sqlalchemy.exc import IntegrityError

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import Zips, ZipsGrouped, StateCongressional
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
zip4_line_size = 182
citystate_line_size = 129
chunk_size = 1024 * 10


def hot_swap_zip_tables(sess):
    """ Drop the existing zips table, rename the temp_zips table, and rename all the indexes in a transaction.

        Args:
            sess: the database connection
    """
    # Getting indexes before dropping the table
    indexes = Zips.__table__.indexes
    grouped_indexes = ZipsGrouped.__table__.indexes

    logger.info("Hot swapping temporary zips table to official zips table.")

    sql_string = """-- Do everything in a transaction so it doesn't affect anything until it's completely done
                    BEGIN;

                    -- Make sure the sequences remain
                    ALTER SEQUENCE zips_zips_id_seq OWNED BY temp_zips.zips_id;
                    ALTER SEQUENCE zips_grouped_zips_grouped_id_seq OWNED BY temp_zips_grouped.zips_grouped_id;

                    -- Drop old zips table and rename the temporary one
                    DROP TABLE zips;
                    DROP TABLE zips_grouped;
                    ALTER TABLE temp_zips RENAME TO zips;
                    ALTER TABLE temp_zips_grouped RENAME TO zips_grouped;

                    -- Rename the PKs and constraints to match what they were in the original zips table
                    ALTER INDEX temp_zips_pkey RENAME TO zips_pkey;
                    ALTER INDEX temp_zips_grouped_pkey RENAME TO zips_grouped_pkey;
                    ALTER INDEX temp_zips_zip5_zip_last4_key RENAME TO uniq_zip5_zip_last4;

                    -- Swap out indexes"""

    # Get all the indexes swapped out
    for index in indexes:
        index_name = index.name.replace('ix_', '')
        sql_string += "\nALTER INDEX temp_{}_idx RENAME TO ix_{};".format(index_name, index_name)
    for index in grouped_indexes:
        index_name = index.name.replace('ix_', '')
        sql_string += "\nALTER INDEX temp_{}_idx RENAME TO ix_{};".format(index_name, index_name)
    sql_string += "COMMIT;"
    sess.execute(sql_string)


def group_zips(sess):
    """ Run SQL to group the zips in the zips table into the zips_grouped table

        Args:
            sess: the database connection
    """
    logger.info("Grouping zips into temporary zips_grouped table.")

    # Inserting basic information
    insert_query = """
        INSERT INTO temp_zips_grouped (zip5, state_abbreviation, county_number)
        SELECT zip5, state_abbreviation, county_number
        FROM temp_zips
        GROUP BY zip5, state_abbreviation, county_number;
    """
    sess.execute(insert_query)
    sess.commit()

    # Updating congressional districts with a count greater than 1 (also
    update_query = """
        WITH district_counts AS (
            SELECT zip5, COUNT(DISTINCT temp_zips.congressional_district_no) AS cd_count
            FROM temp_zips
            GROUP BY zip5)
        UPDATE temp_zips_grouped
        SET created_at = NOW(),
            updated_at = NOW(),
            congressional_district_no = CASE WHEN cd_count <> 1
                                             THEN '90'
                                             END
        FROM district_counts AS dc
        WHERE dc.zip5 = temp_zips_grouped.zip5;
    """
    sess.execute(update_query)
    sess.commit()

    update_query = """
        UPDATE temp_zips_grouped
        SET congressional_district_no = temp_zips.congressional_district_no
        FROM temp_zips
        WHERE temp_zips_grouped.congressional_district_no IS NULL
            AND temp_zips.zip5 = temp_zips_grouped.zip5;
    """
    sess.execute(update_query)
    sess.commit()


def update_state_congr_table_current(sess):
    """ Update contents of state_congressional table based on zips we just inserted

        Args:
            sess: the database connection
    """
    logger.info("Loading zip codes complete, beginning update of state_congressional table")
    # clear old data out
    sess.query(StateCongressional).delete(synchronize_session=False)
    sess.commit()

    # get new data
    distinct_list = sess.query(Zips.state_abbreviation, Zips.congressional_district_no).distinct().\
        order_by(Zips.state_abbreviation, Zips.congressional_district_no)
    sess.bulk_save_objects([StateCongressional(state_code=state_data.state_abbreviation,
                                               congressional_district_no=state_data.congressional_district_no)
                            for state_data in distinct_list])
    sess.commit()


def update_state_congr_table_census(census_file, sess):
    """ Update contents of state_congressional table to include districts from the census

        Args:
            census_file: file path/url to the census file to read
            sess: the database connection
    """
    logger.info("Adding congressional districts from census to the state_congressional table")

    data = pd.read_csv(census_file, dtype=str)
    model = StateCongressional

    data = clean_data(
        data,
        model,
        {"state_code": "state_code",
         "congressional_district_no": "congressional_district_no",
         "census_year": "census_year"},
        {'congressional_district_no': {"pad_to_length": 2}}
    )

    table_name = model.__table__.name
    insert_dataframe(data, table_name, sess.connection())
    sess.commit()


def add_to_table(data, sess):
    """ Add data to the temp_zips table.

        Args:
            data: dictionary of dictionaries containing zip data to process and add to the table
            sess: the database connection
    """
    value_array = []
    for _, item in data.items():
        # Taking care of the nulls so they're actually null in the DB
        zip4 = "'" + item['zip_last4'] + "'" if item['zip_last4'] else 'NULL'
        cd = "'" + item['congressional_district_no'] + "'" if item['congressional_district_no'] else 'NULL'
        value_array.append("(NOW(), NOW(), '{}', {}, '{}', '{}', {})".
                           format(item['zip5'], zip4, item['county_number'], item['state_abbreviation'], cd))
    try:
        if value_array:
            sess.execute('INSERT INTO temp_zips '
                         '(updated_at, created_at, zip5, zip_last4, county_number, state_abbreviation, '
                         'congressional_district_no) VALUES {}'.format(", ".join(value_array)))
            sess.commit()
    except IntegrityError:
        sess.rollback()
        logger.error("Attempted to insert duplicate zip. Inserting each row in batch individually.")

        i = 0
        for new_zip in value_array:
            # create an insert statement that overrides old values if there's a conflict
            sess.execute('INSERT INTO temp_zips '
                         '(updated_at, created_at, zip5, zip_last4, county_number, state_abbreviation, '
                         'congressional_district_no) VALUES {} '
                         'ON CONFLICT DO NOTHING'.format(new_zip))

            # Printing every 1000 rows in each batch
            if i % 1000 == 0:
                logger.info("Inserting row %s of current batch", str(i))
            i += 1
        sess.commit()


def parse_zip4_file(f, sess):
    """ Parse file containing full 9-digit zip data

        Args:
            f: opened file containing zip5 and zip_last4 data
            sess: the database connection
    """
    logger.info("Starting file %s", str(f))
    # pull out the copyright data
    f.read(zip4_line_size)

    data_array = {}
    current_zip = ""
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
        if len(curr_chunk) < zip4_line_size:
            break

        # while we can still do more processing on the current chunk, process it per line
        while len(curr_chunk) >= zip4_line_size:
            # grab another line and get the data that's always the same
            curr_row = curr_chunk[:zip4_line_size]
            state = curr_row[157:159]

            # ignore state codes AA, AE, and AP because they're just for military routing
            if state not in ['AA', 'AE', 'AP']:
                # files are ordered by zip5: when it changes, that's the last record with that zip5
                # insert batches by zip5 to avoid conflicts
                zip5 = curr_row[1:6]
                if current_zip != zip5:
                    if len(data_array) > 0:
                        logger.info("Inserting {} records for {}".format(len(data_array), current_zip))
                        add_to_table(data_array, sess)
                        data_array.clear()
                    current_zip = zip5

                # zip of 96898 is a special case
                if zip5 == "96898":
                    congressional_district = "99"
                    state = "UM"
                    county = "450"
                else:
                    county = curr_row[159:162]
                    congressional_district = curr_row[162:164]

                # certain states require specific CDs
                if state in ["AK", "DE", "MT", "ND", "SD", "VT", "WY"]:
                    congressional_district = "00"
                elif state in ["AS", "DC", "GU", "MP", "PR", "VI"]:
                    congressional_district = "98"
                elif state in ["FM", "MH", "PW", "UM"]:
                    congressional_district = "99"

                try:
                    zip4_low = int(curr_row[140:144])
                    zip4_high = int(curr_row[144:148])
                    # if the zip4 low and zip4 high are the same, it's just one zip code and we can just add it
                    if zip4_low == zip4_high:
                        zip_string = str(zip4_low).zfill(4)
                        data_array[zip5 + zip_string] = {"zip5": zip5, "zip_last4": zip_string, "county_number": county,
                                                         "state_abbreviation": state,
                                                         "congressional_district_no": congressional_district}
                    # if the zip codes are different, we have to loop through and add each zip4
                    # as a different object/key
                    else:
                        i = zip4_low
                        while i <= zip4_high:
                            zip_string = str(i).zfill(4)
                            data_array[zip5 + zip_string] = {"zip5": zip5, "zip_last4": zip_string,
                                                             "state_abbreviation": state, "county_number": county,
                                                             "congressional_district_no": congressional_district}
                            i += 1
                # catch entries where zip code isn't an int (12ND for example, ND stands for "no delivery")
                except ValueError:
                    logger.error("Error parsing entry: %s", curr_row)

            # cut the current line out of the chunk we're processing
            curr_chunk = curr_chunk[zip4_line_size:]

    # add the final chunk of data to the DB
    if len(data_array) > 0:
        logger.info("Adding last {} records for current file".format(len(data_array)))
        add_to_table(data_array, sess)
        data_array.clear()


def parse_citystate_file(f, sess):
    """ Parse citystate file data to get remaining 5-digit zips that weren't included in the 9-digit file

        Args:
            f: opened file containing citystate data
            sess: the database connection
    """
    logger.info("Starting file %s", str(f))
    # pull out the copyright data
    f.read(citystate_line_size)

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
        if len(curr_chunk) < citystate_line_size:
            break

        # while we can still do more processing on the current chunk, process it per line
        while len(curr_chunk) >= citystate_line_size:
            # grab another line and get the data if it's a "detail record"
            curr_row = curr_chunk[:citystate_line_size]
            if curr_row[0] == "D":
                state = curr_row[99:101]

                # ignore state codes AA, AE, and AP because they're just for military routing
                if state not in ['AA', 'AE', 'AP']:
                    zip5 = curr_row[1:6]
                    # zip of 96898 is a special case
                    if zip5 == "96898":
                        congressional_district = "99"
                        state = "UM"
                        county = "450"
                    else:
                        congressional_district = None
                        county = curr_row[101:104]

                    # certain states require specific CDs
                    if state in ["AK", "DE", "MT", "ND", "SD", "VT", "WY"]:
                        congressional_district = "00"
                    elif state in ["AS", "DC", "GU", "MP", "PR", "VI"]:
                        congressional_district = "98"
                    elif state in ["FM", "MH", "PW", "UM"]:
                        congressional_district = "99"

                    data_array[zip5] = {"zip5": zip5, "zip_last4": None, "state_abbreviation": state,
                                        "county_number": county, "congressional_district_no": congressional_district}

            # cut the current line out of the chunk we're processing
            curr_chunk = curr_chunk[citystate_line_size:]

    # remove all zip5s that already exist in the table
    distinct_zip5 = sess.execute('SELECT DISTINCT zip5 FROM temp_zips').fetchall()
    for item in distinct_zip5:
        if item.zip5 in data_array:
            del data_array[item.zip5]

    logger.info("Starting insert on zip5 data")
    add_to_table(data_array, sess)
    return f


def read_zips():
    """ Update zip codes in the zips table. """
    with create_app().app_context():
        sess = GlobalDB.db().session

        # Create temporary table to do work in so we don't disrupt the site for too long by altering the actual table
        sess.execute('CREATE TABLE IF NOT EXISTS temp_zips (LIKE zips INCLUDING ALL);')
        sess.execute('CREATE TABLE IF NOT EXISTS temp_zips_grouped (LIKE zips_grouped INCLUDING ALL);')
        # Truncating in case we didn't clear out this table after a failure in the script
        sess.execute('TRUNCATE TABLE temp_zips;')
        sess.execute('TRUNCATE TABLE temp_zips_grouped;')
        # Resetting the pk sequence
        sess.execute('SELECT setval(\'zips_zips_id_seq\', 1, false);')
        sess.execute('SELECT setval(\'zips_grouped_zips_grouped_id_seq\', 1, false);')
        sess.commit()

        if CONFIG_BROKER["use_aws"]:
            zip_folder = CONFIG_BROKER["zip_folder"] + "/"
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            response = s3_client.list_objects_v2(Bucket=CONFIG_BROKER['sf_133_bucket'], Prefix=zip_folder)
            for obj in response.get('Contents', []):
                if obj['Key'] != zip_folder:
                    zip_4_file_path = s3_client.generate_presigned_url('get_object',
                                                                       {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                        'Key': obj['Key']}, ExpiresIn=600)
                    parse_zip4_file(urllib.request.urlopen(zip_4_file_path), sess)

            # parse remaining 5 digit zips that weren't in the first file
            citystate_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                             'Key': "ctystate.txt"}, ExpiresIn=600)
            parse_citystate_file(urllib.request.urlopen(citystate_file), sess)

            census_file = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                          'Key': "census_congressional_districts.csv"},
                                                           ExpiresIn=600)
        else:
            base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", CONFIG_BROKER["zip_folder"])
            # creating the list while ignoring hidden files on mac
            file_list = [f for f in os.listdir(base_path) if not re.match('^\.', f)]
            for file in file_list:
                parse_zip4_file(open(os.path.join(base_path, file)), sess)

            # parse remaining 5 digit zips that weren't in the first file
            citystate_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "ctystate.txt")
            parse_citystate_file(open(citystate_file), sess)

            census_file = os.path.join(base_path, "census_congressional_districts.csv")

        group_zips(sess)
        hot_swap_zip_tables(sess)
        update_state_congr_table_current(sess)
        update_state_congr_table_census(census_file, sess)

        logger.info("Zipcode script complete")


if __name__ == '__main__':
    configure_logging()
    read_zips()
