import os
import logging
import requests

import pandas as pd
import datetime
import json

from dataactbroker.helpers.pandas_helper import check_dataframe_diff

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CountryCode

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def convert_bool_to_str(b_to_s):
    return str(b_to_s)


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

        country_code_file = 'https://files.usaspending.gov/reference_data/country_codes.csv'
        logger.info('Loading country codes file from {}'.format(country_code_file))

        # Get data from public S3 bucket
        r = requests.get(country_code_file, allow_redirects=True)
        filename = os.path.join(base_path, 'country_codes.csv')
        open(filename, 'wb').write(r.content)

        # Parse data
        data = pd.read_csv(filename, dtype=str)
        metrics_json['records_provided'] = len(data.index)
        data = clean_data(
            data,
            CountryCode,
            {"country_code": 'country_code',
             'country_name': 'country_name',
             'territory_or_freely_associated_state': 'territory_free_state'},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['country_code'], inplace=True)
        metrics_json['duplicates_dropped'] = metrics_json['records_provided'] - len(data.index)

        # compare to existing content in table
        diff_found = check_dataframe_diff(data, CountryCode, 'country_code_id', ['country_code'],
                                          lambda_funcs={'territory_free_state': convert_bool_to_str})

        # insert to db if reload required
        if force_reload or diff_found:
            logger.info('Differences found or reload forced, reloading country_code table.')
            # if there's a difference, clear out the old data before adding the new stuff
            metrics_json['records_deleted'] = sess.query(CountryCode).delete()

            num = insert_dataframe(data, CountryCode.__table__.name, sess.connection())
            metrics_json['records_inserted'] = num
            sess.commit()

            logger.info('{} records inserted to country_code table'.format(num))
        else:
            logger.info('No differences found, skipping country_code table reload.')

        # Delete file once we're done
        os.remove(filename)

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('load_country_codes_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")
