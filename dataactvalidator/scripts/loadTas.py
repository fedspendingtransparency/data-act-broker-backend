from collections import defaultdict
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import TASLookup
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data


logger = logging.getLogger(__name__)


def clean_tas(csv_path):
    """ Read a CSV into a dataframe, then use a configured `clean_data` and return the results """
    data = pd.read_csv(csv_path, dtype=str)
    data = clean_data(
        data,
        TASLookup,
        {"a": "availability_type_code",
         "acct_num": "account_num",
         "aid": "agency_identifier",
         "ata": "allocation_transfer_agency",
         "bpoa": "beginning_period_of_availa",
         "epoa": "ending_period_of_availabil",
         "main": "main_account_code",
         "sub": "sub_account_code",
         "financial_indicator_type2": "financial_indicator2",
         "dt_tm_estab": "internal_start_date",
         "dt_end": "internal_end_date",
         "fr_entity_description": "fr_entity_description",
         "fr_entity_type": "fr_entity_type",
         "gwa_tas_name": "account_title",
         "agency_aid": "reporting_agency_aid",
         "agency_name": "reporting_agency_name",
         "admin_org": "budget_bureau_code",
         "admin_org_name": "budget_bureau_name",
         "function_code": "budget_function_code",
         "function_description": "budget_function_title",
         "sub_function_code": "budget_subfunction_code",
         "sub_function_description": "budget_subfunction_title"
         },
        {"allocation_transfer_agency": {"pad_to_length": 3, "keep_null": True},
         "agency_identifier": {"pad_to_length": 3},
         # Account for " " cells
         "availability_type_code": {"pad_to_length": 0, "keep_null": True},
         "beginning_period_of_availa": {"pad_to_length": 0, "keep_null": True},
         "ending_period_of_availabil": {"pad_to_length": 0, "keep_null": True},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3},
         }
    )
    data["account_num"] = pd.to_numeric(data['account_num'])
    return data.where(pd.notnull(data), None)


def update_tas_lookups(csv_path, only_fill_in=False):
    """ Load TAS data from the provided CSV and replace/insert any TASLookups """
    sess = GlobalDB.db().session

    data = clean_tas(csv_path)
    add_existing_id(data)

    old_data = data[data['existing_id'].notnull()]
    del old_data['existing_id']

    new_data = data[data['existing_id'].isnull()]
    del new_data['existing_id']

    if only_fill_in:
        # Pull in the empty records
        empty_records = sess.query(TASLookup).filter_by(budget_function_code=None).values('account_num')
        empty_account_nums = [int(empty_record[0]) for empty_record in empty_records]
        # find which incoming records can fill in the empty records
        relevant_old_data = old_data[old_data['account_num'].isin(empty_account_nums)]
        # Fill them in. If budget_function_code is empty, the following columns have also been empty.
        fill_in_cols = ['account_title', 'budget_bureau_code', 'budget_bureau_name', 'budget_function_code',
                        'budget_function_title', 'budget_subfunction_code', 'budget_subfunction_title',
                        'reporting_agency_aid', 'reporting_agency_name']
        for _, row in relevant_old_data.iterrows():
            fill_in_updates = {fill_in_col: row[fill_in_col] for fill_in_col in fill_in_cols}
            fill_in_updates['updated_at'] = datetime.now(timezone.utc)
            sess.query(TASLookup).filter_by(account_num=row['account_num']).update(synchronize_session=False,
                                                                                   values=fill_in_updates)
        logger.info('%s records filled in', len(relevant_old_data.index))
    else:
        # instead of using the pandas to_sql dataframe method like some of the
        # other domain load processes, iterate through the dataframe rows so we
        # can load using the orm model (note: toyed with the SQLAlchemy bulk load
        # options but ultimately decided not to go outside the unit of work for
        # the sake of a performance gain)
        for _, row in old_data.iterrows():
            sess.query(TASLookup).filter_by(account_num=row['account_num']).update(row, synchronize_session=False)

        for _, row in new_data.iterrows():
            sess.add(TASLookup(**row))
        logger.info('%s records in CSV, %s existing', len(data.index), sum(data['existing_id'].notnull()))

    sess.commit()


def load_tas(tas_file=None, only_fill_in=False):
    """ Load TAS file into broker database. """
    # read TAS file to dataframe, to make sure all is well
    # with the file before firing up a db transaction
    if not tas_file:
        if CONFIG_BROKER["use_aws"]:
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
            tas_file = s3bucket.get_key("cars_tas.csv").generate_url(expires_in=600)
        else:
            tas_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "cars_tas.csv")

    with create_app().app_context():
        update_tas_lookups(tas_file, only_fill_in=only_fill_in)


def add_existing_id(data):
    """ Look up the ids of existing TASes. Use account_num as a non-unique identifier to help filter results """
    existing = defaultdict(list)
    query = GlobalDB.db().session.query(TASLookup).\
        filter(TASLookup.account_num.in_(int(i) for i in data['account_num']))
    for tas in query:
        existing[tas.account_num].append(tas)

    data['existing_id'] = data.apply(existing_id, axis=1, existing=existing)


def existing_id(row, existing):
    """ Check for a TASLookup which matches this `row` in the `existing` data.
        Args:
            row: row to check in
            existing: Dict[account_num, List[TASLookup]]
    """
    for potential_match in existing[row['account_num']]:
        return potential_match.account_num


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    tas_parser = argparse.ArgumentParser(description='Import data from the cars_tas.csv')
    tas_parser.add_argument('--tas_file', '-f', type=str, default=None, help='Path to specifc cars_tas file to use')
    tas_parser.add_argument('--only_fill_in', '-o', action='store_true', help='Only fill in records with missing data')
    return tas_parser

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    configure_logging()

    tas_file = args.tas_file
    only_fill_in = args.only_fill_in
    if tas_file and not os.path.exists(tas_file):
        logger.error('File does not exist: {}'.format(tas_file))
    else:
        load_tas(tas_file=tas_file, only_fill_in=only_fill_in)
