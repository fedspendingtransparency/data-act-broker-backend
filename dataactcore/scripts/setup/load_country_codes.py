import logging
import requests
import xmltodict
import time
import csv
import boto3
import os

import pandas as pd
import datetime
import json

from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError

from dataactbroker.helpers.pandas_helper import check_dataframe_diff

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.models.domainModels import CountryCode
from dataactcore.utils.loader_utils import insert_dataframe

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

CC_NAMESPACES = {'http://api.nsgreg.nga.mil/schema/genc/3.0': None,
                 'http://api.nsgreg.nga.mil/schema/genc/3.0/genc-cmn': None}

TERRITORY_LIST = ['ASM', 'FSM', 'GUM', 'MHL', 'MNP', 'PLW', 'PRI', 'VIR', 'XBK', 'XHO', 'XJA', 'XJV', 'XKR', 'XMW',
                  'XNV', 'XPL', 'XWK']


def convert_territory_bool_to_str(row):
    return str(row['territory_free_state'])


def list_data(data):
    if isinstance(data, dict):
        # make a list so it's consistent
        data = [data, ]
    return data


def get_with_exception_hand(url_string):
    """ Retrieve data from Country Code, allow for multiple retries and timeouts """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while exception_retries < len(retry_sleep_times):
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            break
        except (ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout, KeyError) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception. Sleeping {}s and then retrying with a max wait of {}s...'
                            .format(retry_sleep_times[exception_retries], request_timeout))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to Country Code feed lost, maximum retry attempts exceeded.')
                raise e
    return resp


def load_country_codes(base_path, force_reload=False):
    """ Load Country Codes into the database.

        Args:
            base_path: directory that contains the domain values files.
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    now = datetime.datetime.now()
    metrics_json = {
        'script_name': 'load_country_codes.py',
        'start_time': str(now),
        'records_deleted': 0,
        'records_provided': 0,
        'duplicates_dropped': 0,
        'records_inserted': 0
    }

    with create_app().app_context():
        sess = GlobalDB.db().session
        feed_url = 'https://nsgreg-api.nga.mil/geo-political/GENC/3/now'

        resp = get_with_exception_hand(feed_url)

        resp_dict = xmltodict.parse(resp.text, process_namespaces=True, namespaces=CC_NAMESPACES)
        country_data = list_data(resp_dict['GENCStandardBaseline']['GeopoliticalEntityEntry'])
        country_list = []

        for country in country_data:
            country_list.append({
                'country_name': country['name'],
                'country_code': country['encoding']['char3Code'],
                'country_code_2_char': country['encoding']['char2Code'],
                'territory_free_state': country['encoding']['char3Code'] in TERRITORY_LIST
            })

        data = pd.DataFrame(country_list)
        diff_found = check_dataframe_diff(data, CountryCode, ['country_code_id'], ['country_code'],
                                          lambda_funcs=[('territory_free_state', convert_territory_bool_to_str)])

        # insert to db if reload required
        if force_reload or diff_found:
            logger.info('Differences found or reload forced, reloading country_code table.')
            # if there's a difference, clear out the old data before adding the new stuff
            metrics_json['records_deleted'] = sess.query(CountryCode).delete()

            num = insert_dataframe(data, CountryCode.__table__.name, sess.connection())
            metrics_json['records_inserted'] = num
            sess.commit()

            if CONFIG_BROKER["use_aws"]:
                cc_filename = 'country_codes.csv'

                data.to_csv(cc_filename, index=False, quoting=csv.QUOTE_ALL, header=True,
                            columns=['country_code', 'country_code_2_char', 'country_name', 'territory_free_state'])

                logger.info("Uploading {} to {}".format(cc_filename, CONFIG_BROKER["public_files_bucket"]))
                s3 = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
                s3.upload_file('country_codes.csv', CONFIG_BROKER["public_files_bucket"],
                               'broker_reference_data/country_codes.csv')
                os.remove(cc_filename)

            # Updating data load dates if the load successfully added new country codes
            update_external_data_load_date(now, datetime.datetime.now(), 'country_code')

            logger.info('{} records inserted to country_code table'.format(num))
        else:
            logger.info('No differences found, skipping country_code table reload.')

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('load_country_codes_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info('Script complete')
