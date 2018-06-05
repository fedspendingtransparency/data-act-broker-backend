import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import ProgramActivity
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def get_program_activity_file(base_path):
    """ Retrieves the program activity file to load

        Args:
            base_path: directory of domain config files

        Returns:
            Returns the file path for the pa file either on S3 or locally
    """

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        pa_file = s3bucket.get_key("program_activity.csv").generate_url(expires_in=600)
    else:
        pa_file = os.path.join(base_path, "program_activity.csv")

    return pa_file


def load_program_activity_data(base_path):
    """ Load program activity lookup table.

        Args:
            base_path: directory of domain config files
    """

    program_activity_file = get_program_activity_file(base_path)

    logger.info('Loading program activity: program_activity.csv')

    with create_app().app_context():
        sess = GlobalDB.db().session

        sess.query(ProgramActivity).delete()

        data = pd.read_csv(program_activity_file, dtype=str)
        data = clean_data(
            data,
            ProgramActivity,
            {"fyq": "fiscal_year_quarter", "agency_id": "agency_id", "alloc_id": "allocation_transfer_id",
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
        table_name = ProgramActivity.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())

        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))
