import os
import logging

import pandas as pd
import numpy as np
import boto3
import datetime
import json

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CountryCode
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


# Territories or freely associated states
TERRITORIES_FREE_STATES = ["ASM", "XBK", "GUM", "XHO", "XJV", "XJA", "XKR", "XMW", "XNV", "MNP", "PRI", "XPL", "VIR",
                           "XWK", "PLW", "FSM", "MHL"]


def load_country_codes(base_path):
    """ Load Country Codes into the database.

        Args:
            base_path: directory that contains the domain values files.
    """
    now = datetime.datetime.now()
    metrics_json = {
        'script_name': 'load_country_codes.py',
        'records_deleted': 0,
        'records_provided': 0,
        'duplicates_dropped': 0,
        'records_inserted': 0,
        'start_time': str(now)
    }
    if CONFIG_BROKER["use_aws"]:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        filename = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                   'Key': "country_codes.csv"}, ExpiresIn=600)
    else:
        filename = os.path.join(base_path, "country_codes.csv")

    logger.info('Loading country codes file: country_codes.csv')

    with create_app().app_context():
        sess = GlobalDB.db().session

        # for object class, delete and replace values
        metrics_json['records_deleted'] = sess.query(CountryCode).delete()

        data = pd.read_csv(filename, dtype=str)
        metrics_json['records_provided'] = len(data.index)
        data = clean_data(
            data,
            CountryCode,
            {"country_code": "country_code", "country_name": "country_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['country_code'], inplace=True)
        metrics_json['duplicates_dropped'] = metrics_json['records_provided'] - len(data.index)
        # flag territories or freely associated states
        data["territory_free_state"] = np.where(data["country_code"].isin(TERRITORIES_FREE_STATES), True, False)
        # insert to db
        table_name = CountryCode.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        metrics_json['records_inserted'] = num
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('load_country_codes_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")
