# Local tests took 1 hour to insert 5 million lines. Estimated 9 hours to insert all data as of 05/2017.
# Suggested to run on a weekend during off hours

import os
import re
import logging
import boto
import urllib.request

from sqlalchemy.dialects.postgresql import insert

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import Zips

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
line_size = 182
chunk_size = 1024 * 10


# add data to the zips table
def add_to_table(data, sess):
    i = 0
    # loop through all the items in the current array
    for _, new_zip in data.items():
        # create an insert statement that overrides old values if there's a conflict
        insert_statement = insert(Zips).values(**new_zip). \
            on_conflict_do_update(index_elements=[Zips.zip5, Zips.zip_last4],
                                  set_=dict(state_abbreviation=new_zip["state_abbreviation"],
                                            county_number=new_zip["county_number"],
                                            congressional_district_no=new_zip["congressional_district_no"]))
        sess.execute(insert_statement)

        if i % 10000 == 0:
            logger.info("inserting row " + str(i) + " of current batch")
        i += 1
    sess.commit()


def parse_zip4_file(f, sess):
    logger.info("starting file " + str(f))
    # pull out the copyright data
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
            # grab another line and get the data that's always the same
            curr_row = curr_chunk[:line_size]
            zip5 = curr_row[1:6]
            state = curr_row[157:159]
            county = curr_row[159:162]
            congressional_district = curr_row[162:164]

            try:
                zip4_low = int(curr_row[140:144])
                zip4_high = int(curr_row[144:148])
                # if the zip4 low and zip4 high are the same, it's just one zip code and we can just add it
                if zip4_low == zip4_high:
                    zip_string = str(zip4_low).zfill(4)
                    data_array[zip5 + zip_string] = {"zip5": zip5, "zip_last4": zip_string, "state_abbreviation": state,
                                                     "county_number": county,
                                                     "congressional_district_no": congressional_district}
                # if the zip codes are different, we have to loop through and add each zip4 as a different object/key
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
                logger.error("error parsing entry: " + curr_row)

            # cut the current line out of the chunk we're processing
            curr_chunk = curr_chunk[line_size:]

        # we want to do DB adding in large chunks so we can hopefully remove duplicates that are near each other
        # in the file just by them having the same key in the dict
        if len(data_array) > 100000:
            logger.info("inserting next 100k+ records")
            add_to_table(data_array, sess)
            data_array.clear()

    # add the final chunk of data to the DB
    if len(data_array) > 0:
        logger.info("adding last set of records for current file")
        add_to_table(data_array, sess)
        data_array.clear()


def main():
    sess = GlobalDB.db().session

    # delete old values in case something changed and one is now invalid
    sess.query(Zips).delete(synchronize_session=False)
    sess.commit()

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        zip_folder = CONFIG_BROKER["zip_folder"] + "/"
        for key in s3bucket.list(prefix=zip_folder):
            if key.name != zip_folder:
                zip_4_file_path = key.generate_url(expires_in=600)
                parse_zip4_file(urllib.request.urlopen(zip_4_file_path), sess)
    else:
        base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", CONFIG_BROKER["zip_folder"])
        file_list = [f for f in os.listdir(base_path)]
        for file in file_list:
            # ignore hidden files on Mac
            if not re.match('^\.', file):
                parse_zip4_file(open(os.path.join(base_path, file)), sess)

    logger.info("Zipcode script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
