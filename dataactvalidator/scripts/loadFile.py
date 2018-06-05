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


def load_country_codes(filename):
    """Load country code lookup table."""
    model = CountryCode

    with create_app().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"country_code": "country_code", "country_name": "country_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['country_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def load_domain_values(base_path):
    """Load all domain value files.

    Parameters
    ----------
        base_path : directory that contains the domain values files.
    """
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        country_codes_file = s3bucket.get_key("country_codes.csv").generate_url(expires_in=600)
    else:
        country_codes_file = os.path.join(base_path, "country_codes.csv")

    logger.info('Loading country codes file: ' + "country_codes.csv")
    load_country_codes(country_codes_file)


if __name__ == '__main__':
    configure_logging()
    load_domain_values(os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
