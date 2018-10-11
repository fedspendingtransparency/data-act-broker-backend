from collections import defaultdict
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
import boto3

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import TASLookup
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data


logger = logging.getLogger(__name__)

unchanged_columns = {
    "a": "availability_type_code",
    "acct_num": "account_num",
    "aid": "agency_identifier",
    "ata": "allocation_transfer_agency",
    "bpoa": "beginning_period_of_availa",
    "epoa": "ending_period_of_availabil",
    "main": "main_account_code",
    "sub": "sub_account_code",
    "fr_entity_description": "fr_entity_description",
    "gwa_tas_name": "account_title",
    "agency_aid": "reporting_agency_aid",
    "agency_name": "reporting_agency_name",
    "admin_org": "budget_bureau_code",
    "admin_org_name": "budget_bureau_name",
    "function_code": "budget_function_code",
    "function_description": "budget_function_title",
    "sub_function_code": "budget_subfunction_code",
    "sub_function_description": "budget_subfunction_title",
}
original = {
    "financial_indicator_type_2": "financial_indicator2",
    "date/time_established": "internal_start_date",
    "end_date": "internal_end_date",
    "fr_entity_type_code": "fr_entity_type"
}
current = {
    "financial_indicator_type2": "financial_indicator2",
    "dt_tm_estab": "internal_start_date",
    "dt_end": "internal_end_date",
    "fr_entity_type": "fr_entity_type"
}
original_mappings = {**unchanged_columns, **original}
current_mappings = {**unchanged_columns, **current}


def clean_tas(csv_path):
    """ Read a CSV into a dataframe, then use a configured `clean_data` and return the results

        Args:
            csv_path: path of the car_tas csv to import

        Returns:
            pandas dataframe of clean data imported from the cars_tas csv
    """
    # Encoding accounts for cases where a column may include '\ufeff'
    data = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
    for column_mappings in [current_mappings, original_mappings]:
        try:
            data = clean_data(
                data,
                TASLookup,
                column_mappings,
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
            break
        except ValueError as e:
            if column_mappings != original_mappings:
                logger.info('Mismatched columns, trying again with different column set')
            else:
                logger.error('Encountered new column set: {}'.format(data.columns))
                raise e
    data["account_num"] = pd.to_numeric(data['account_num'])
    return data.where(pd.notnull(data), None)


def update_tas_lookups(sess, csv_path, update_missing=[]):
    """ Load TAS data from the provided CSV and replace/insert any TASLookups

        Args:
            sess: connection to database
            csv_path: path of the car_tas csv to import
            update_missing: if provided, this list of account numbers will only update matching records
                            if the budget_function_code is null/none
    """
    data = clean_tas(csv_path)
    add_existing_id(data)

    old_data = data[data['existing_id'].notnull()]
    del old_data['existing_id']

    new_data = data[data['existing_id'].isnull()]
    del new_data['existing_id']

    if update_missing:
        # find which incoming records can fill in the empty records
        relevant_old_data = old_data[old_data['account_num'].isin(update_missing)]
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
        # instead of using the pandas to_sql dataframe method like some of the other domain load processes, iterate
        # through the dataframe rows so we can load using the orm model (note: toyed with the SQLAlchemy bulk load
        # options but ultimately decided not to go outside the unit of work for the sake of a performance gain)
        for _, row in old_data.iterrows():
            sess.query(TASLookup).filter_by(account_num=row['account_num']).update(row, synchronize_session=False)

        for _, row in new_data.iterrows():
            sess.add(TASLookup(**row))
        logger.info('%s records in CSV, %s existing', len(data.index), sum(data['existing_id'].notnull()))

    sess.commit()


def load_tas(backfill_historic=False):
    """ Load TAS file into broker database.

        Args:
            backfill_historic: if set to true, this will only update certain columns if budget_function_code is null
    """
    # read TAS file to dataframe, to make sure all is well with the file before firing up a db transaction
    sess = GlobalDB.db().session
    tas_files = []

    if CONFIG_BROKER["use_aws"]:
        # Storing version dictionaries in the list to prevent getting all the links at once and possibly work with
        # expired AWS links
        s3connection = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        # list_object_versions returns the versions in reverse chronological order

        if not backfill_historic:
            # get the latest tas_file
            tas_files = [s3connection.generate_presigned_url(ClientMethod='get_object',
                                                             Params={'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                     'Key': 'cars_tas.csv'}, ExpiresIn=600)]
        else:
            tas_files = s3connection.list_object_versions(Bucket=CONFIG_BROKER['sf_133_bucket'],
                                                          Prefix='cars_tas.csv')['Versions']
            # getting the latest file (see the reversed) from each day for performance and accuracy
            tas_files_grouped = {tas_file['LastModified'].date(): tas_file for tas_file in reversed(tas_files)}
            # sorting them back to chronological order
            tas_files = sorted([tas_file for date, tas_file in tas_files_grouped.items()],
                               key=lambda k: k['LastModified'])
    elif backfill_historic:
        logger.error('Unable to attain historical versions of cars_tas without aws access.')
        return
    else:
        tas_file = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "cars_tas.csv")
        tas_files.append(tas_file)

    for tas_file in reversed(tas_files):
        update_missing = missing_records(sess) if backfill_historic else []
        if backfill_historic and not update_missing:
            # no more missing, we're done here
            break
        if CONFIG_BROKER["use_aws"] and backfill_historic:
            # generate url from the version dict
            logger.info('Working with remote cars_tas.csv (from {})'.format(tas_file['LastModified']))
            tas_file = s3connection.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': CONFIG_BROKER['sf_133_bucket'],
                    'Key': 'cars_tas.csv',
                    'VersionId': tas_file['VersionId']
                },
                ExpiresIn=600
            )
        elif CONFIG_BROKER["use_aws"]:
            logger.info('Working with latest remote cars_tas.csv')
        else:
            logger.info('Working with local cars_tas.csv')
        update_tas_lookups(sess, tas_file, update_missing=update_missing)


def add_existing_id(data):
    """ Look up the ids of existing TASes. Use account_num as a non-unique identifier to help filter results

        Args:
            data: dataframe of imported cars_tas
    """
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
        Returns:
            account number for existing row
    """
    for potential_match in existing[row['account_num']]:
        return potential_match.account_num


def missing_records(sess):
    """ Pull in the empty records

        Args:
            sess: connection to the database
        Returns:
            list of account numbers representing records with empty values
    """
    empty_records = sess.query(TASLookup).filter_by(budget_function_code=None).values('account_num')
    return [int(empty_record[0]) for empty_record in empty_records]


if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        parser = argparse.ArgumentParser(description='Import data from the cars_tas.csv')
        parser.add_argument('--backfill_historic', '-b', action='store_true', help='Backfill tas with historical data')
        args = parser.parse_args()

        load_tas(backfill_historic=args.backfill_historic)
