import logging
import datetime
import pandas as pd
import numpy as np
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import trim_item
from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter

logger = logging.getLogger(__name__)


def get_delete_file():
    """ Read the file into a pandas object """

    file_name = 'IDV_Deletes.csv'

    if CONFIG_BROKER["use_aws"]:
        reader = CsvReader()
        pa_file = open(reader.get_filename(CONFIG_BROKER['aws_region'], CONFIG_BROKER['sf_133_bucket'], file_name),
                       encoding='utf-8')
    else:
        base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
        pa_file = os.path.join(base_path, file_name)

    return pa_file


def convert_date(date_string):
    """ Converts the date to the same format as our last_modified column """
    delete_date = datetime.datetime.strptime(date_string, '%m/%d/%y %I:%M %p')
    date_string = delete_date.strftime('%Y-%m-%d %H:%M:%S')
    return date_string


def convert_unique_key(unique_key):
    """ Converts the unique key given by the file into the format we use for our unique key """
    unique_key_array = unique_key.split(':')
    unique_key = unique_key_array[2] + '_-none-_' + unique_key_array[0] + '_' + unique_key_array[1] + '_-none-_-none-'
    return unique_key


def clean_delete_data(data):
    """ Clean up the data so it's easier to process """
    # Shouldn't be any extra rows, but just in case, drop all with no contents
    data.dropna(inplace=True, how='all')

    # replace NaN
    data = data.replace(np.nan, '', regex=True)

    # trim all columns
    data = data.applymap(lambda x: trim_item(x) if len(str(x).strip()) else None)

    # Convert all dates to the same format as we have in the DB
    data['delete_date'] = data['delete_date'].map(lambda x: convert_date(x) if x else None)

    # Convert all unique keys to the same format as we have in the DB
    data['primary_key'] = data['primary_key'].map(lambda x: convert_unique_key(x) if x else None)

    return data


def get_deletes(sess, data):
    """ Gets all the values that actually need to be deleted from our database """
    model = DetachedAwardProcurement
    delete_dict = {}
    delete_list = []
    row_count = len(data.index)
    for index, row in data.iterrows():
        unique_string = row['primary_key']
        last_modified = row['delete_date']

        # Keeping track so we know it isn't spinning its wheels forever
        if index % 500 == 0:
            logger.info("Checking delete record {} of {}.".format(index, row_count))

        existing_item = sess.query(model.last_modified, model.detached_award_procurement_id,
                                   model.detached_award_proc_unique). \
            filter_by(detached_award_proc_unique=unique_string).one_or_none()

        if existing_item and last_modified > existing_item.last_modified:
            delete_list.append(existing_item.detached_award_procurement_id)
            delete_dict[existing_item.detached_award_procurement_id] = existing_item.detached_award_proc_unique

    return delete_list, delete_dict


def delete_records(sess, delete_list, delete_dict):
    """ Delete the records listed and create a file for website deletion. """

    # only need to delete values if there's something to delete
    if delete_list:
        sess.query(DetachedAwardProcurement). \
            filter(DetachedAwardProcurement.detached_award_procurement_id.in_(delete_list)). \
            delete(synchronize_session=False)

    # writing the file
    seconds = int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    now = datetime.datetime.now()
    file_name = now.strftime('%m-%d-%Y') + "_delete_records_IDV_" + str(seconds) + ".csv"
    headers = ["detached_award_procurement_id", "detached_award_proc_unique"]
    if CONFIG_BROKER["use_aws"]:
        with CsvS3Writer(CONFIG_BROKER['aws_region'], CONFIG_BROKER['fpds_delete_bucket'], file_name,
                         headers) as writer:
            for key, value in delete_dict.items():
                writer.write([key, value])
            writer.finish_batch()
    else:
        with CsvLocalWriter(file_name, headers) as writer:
            for key, value in delete_dict.items():
                writer.write([key, value])
            writer.finish_batch()


def main():
    sess = GlobalDB.db().session
    start = datetime.datetime.now()
    logger.info("FPDS IDV delete started")

    # get and read the file
    del_file = get_delete_file()
    data = pd.read_csv(del_file, dtype=str, encoding='utf_8_sig')

    # Clean up the data so it's usable
    data = clean_delete_data(data)

    # Gather list of records to delete
    gather_start = datetime.datetime.now()
    logger.info("Starting gathering of records to delete.")
    delete_list, delete_dict = get_deletes(sess, data)
    gather_end = datetime.datetime.now()
    logger.info("Finished gathering records in {} seconds. Total records to delete: {}".format(gather_end-gather_start,
                                                                                               len(delete_list)))
    # Delete records
    logger.info("Deleting records")
    delete_records(sess, delete_list, delete_dict)
    sess.commit()

    end = datetime.datetime.now()
    logger.info("FPDS IDV delete finished in %s seconds", end-start)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
