import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CountryCode
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_country_codes(base_path):
    """Load all domain value files.

    Args
        base_path: directory that contains the domain values files.
    """

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        filename = s3bucket.get_key("country_codes.csv").generate_url(expires_in=600)
    else:
        filename = os.path.join(base_path, "country_codes.csv")

    logger.info('Loading country codes file: ' + "country_codes.csv")

    with create_app().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(CountryCode).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            CountryCode,
            {"country_code": "country_code", "country_name": "country_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['country_code'], inplace=True)
        # insert to db
        table_name = CountryCode.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))

if __name__ == '__main__':
    configure_logging()
