import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import ObjectClass, ProgramActivity, CountryCode, CFDAProgram
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe, format_date

logger = logging.getLogger(__name__)


def load_object_class(filename):
    """Load object class lookup table."""
    model = ObjectClass

    with create_app().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
            {"object_class_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} Records Inserted To {}'.format(num, table_name))


def load_program_activity(filename):
    """Load program activity lookup table."""
    model = ProgramActivity

    with create_app().app_context():
        sess = GlobalDB.db().session

        # for program activity, delete and replace values??
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"year": "budget_year", "agency_id": "agency_id", "alloc_id": "allocation_transfer_id",
             "account": "account_number", "pa_code": "program_activity_code", "pa_name": "program_activity_name"},
            {"program_activity_code": {"pad_to_length": 4}, "agency_id": {"pad_to_length": 3},
             "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True}, "account_number": {"pad_to_length": 4}}
        )
        # Lowercase Program Activity Name
        data['program_activity_name'] = data['program_activity_name'].apply(lambda x: x.lower())
        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load.
        data.drop_duplicates(inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} Records Inserted To {}'.format(num, table_name))


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

    logger.info('{} Records Inserted To {}'.format(num, table_name))

def load_domain_values_temp(base_path, local_program_activity=None):
    """Load all domain value files.

    Parameters
    ----------
        base_path : directory that contains the domain values files.
        local_program_activity : optional location of the program activity file (None = use basePath)
    """
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        object_class_file = s3bucket.get_key("object_class.csv").generate_url(expires_in=600)
        program_activity_file = s3bucket.get_key("program_activity.csv").generate_url(expires_in=600)
        country_codes_file = s3bucket.get_key("country_codes.csv").generate_url(expires_in=600)
    else:
        object_class_file = os.path.join(base_path, "object_class.csv")
        program_activity_file = os.path.join(base_path, "program_activity.csv")
        country_codes_file = os.path.join(base_path, "country_codes.csv")

    logger.info('Loading Object Class')
    load_object_class(object_class_file)
    logger.info('Loading Country Codes')
    load_country_codes(country_codes_file)
    logger.info('Loading Program Activity')

    if local_program_activity is not None:
        load_program_activity(local_program_activity)
    else:
        load_program_activity(program_activity_file)


if __name__ == '__main__':
    configure_logging()
    load_domain_values_temp(os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
