import argparse
import datetime
import logging
import numpy as np
import pandas as pd

from dataactbroker.helpers.uri_helper import RetrieveFileFromUri

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.scripts.pull_fpds_data import create_lookups, get_data

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def get_param_cols(columns):
    """ Get the columns that were provided in the file and return that list so we don't try to query non-existent cols

        Args:
            columns: The columns in the header of the provided file

        Returns:
            A dict containing all the FPDS query columns that the provided file has
    """
    possible_cols = {'agency_id': 'AGENCY_CODE:"{}" ', 'referenced_idv_agency_iden': 'REF_IDV_AGENCY_ID:"{}" ',
                     'piid': 'PIID:"{}" ', 'award_modification_amendme': 'MODIFICATION_NUMBER:"{}" ',
                     'parent_award_id': 'REF_IDV_PIID:"{}" ', 'transaction_number': 'TRANSACTION_NUMBER:"{}" ',
                     'test_key': "This is a test"}
    existing_cols = {}

    for key in possible_cols:
        if key in columns:
            existing_cols[key] = possible_cols[key]

    return existing_cols


def create_param_string(data, cols):
    """ Create a param string given the provided data and the columns that were determined to exist in the provided
        csv. We don't want NULL values to be inserted into the param string.

        Args:
            data: One row of data from the provided csv
            cols: The columns that have been determined to exist in this csv

        Returns:
            A dict containing all the FPDS query columns that the provided file has
    """
    param_string = ''
    for col in cols:
        if data[col]:
            param_string += cols[col].format(data[col])

    return param_string


if __name__ == '__main__':
    sess = GlobalDB.db().session

    now = datetime.datetime.now()
    configure_logging()

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-f', '--file', help='The uri to the file to use', required=True, type=str, nargs=1)
    args = parser.parse_args()

    with create_app().app_context():
        with RetrieveFileFromUri(args.file[0], 'r').get_file_object() as fix_file:
            keys_to_fix_df = pd.read_csv(fix_file, dtype=str)

        # Fix the nulls
        keys_to_fix_df = keys_to_fix_df.replace(np.nan, '', regex=True)

        file_columns = keys_to_fix_df.columns

        param_cols = get_param_cols(file_columns)

        # Make sure we have at least one valid column to query with
        if len(param_cols) == 0:
            logger.error('No columns provided that can be used to create a query for FPDS, exiting.')
            raise ValueError('No columns provided that can be used to create a query for FPDS, exiting.')

        # Make arrays for looping
        award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
        award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]

        sub_tier_list, country_list, state_code_list, county_by_name, county_by_code,\
            exec_comp_dict = create_lookups(sess)

        # Query FPDS for every unique row in the provided csv
        for _, row in keys_to_fix_df.iterrows():
            params = create_param_string(row, param_cols)

            for award_type in award_types_idv:
                get_data("IDV", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                         country_list, exec_comp_dict, specific_params=params)

            for award_type in award_types_award:
                get_data("award", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                         country_list, exec_comp_dict, specific_params=params)
