import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import ObjectClass
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_object_class(base_path):

    """ This function loads Object classes into the database
        Args:
            base_path : directory that contains the domain values files.
    """
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        filename = s3bucket.get_key("object_class.csv").generate_url(expires_in=600)
    else:
        filename = os.path.join(base_path, "object_class.csv")

    # Load object class lookup table
    logger.info('Loading Object Class File: object_class.csv')
    with create_app().app_context():
        sess = GlobalDB.db().session
        sess.query(ObjectClass).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            ObjectClass,
            {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
            {"object_class_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        # insert to db
        table_name = ObjectClass.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


if __name__ == '__main__':
    configure_logging()
