import os
import logging
import json

import pandas as pd
import boto3
import datetime

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import ObjectClass
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_object_class(base_path):
    """ This function loads Object classes into the database

        Args:
            base_path: directory that contains the domain values files.
    """
    now = datetime.datetime.now()
    metrics_json = {
        'script_name': 'load_object_class.py',
        'start_time': str(now),
        'records_received': 0,
        'duplicates_dropped': 0,
        'records_deleted': 0,
        'records_inserted': 0
    }
    if CONFIG_BROKER["use_aws"]:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        filename = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                   'Key': "object_class.csv"}, ExpiresIn=600)
    else:
        filename = os.path.join(base_path, "object_class.csv")

    # Load object class lookup table
    logger.info('Loading Object Class File: object_class.csv')
    with create_app().app_context():
        sess = GlobalDB.db().session
        metrics_json['records_deleted'] = sess.query(ObjectClass).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            ObjectClass,
            {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
            {"object_class_code": {"pad_to_length": 3}}
        )
        metrics_json['records_received'] = len(data.index)
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        metrics_json['duplicates_dropped'] = metrics_json['records_received'] - len(data.index)
        # insert to db
        table_name = ObjectClass.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))
    metrics_json['records_inserted'] = num

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('load_object_class_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)


if __name__ == '__main__':
    configure_logging()
