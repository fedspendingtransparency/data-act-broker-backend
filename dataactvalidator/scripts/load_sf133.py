from collections import namedtuple
from datetime import date, datetime
import glob
import logging
import os
import re
import json
import argparse

import boto3
import pandas as pd

from dataactbroker.helpers.generic_helper import format_internal_tas
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import (matching_cars_subquery, SF133, TASLookup, TAS_COMPONENTS,
                                             concat_display_tas_dict)
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_all_sf133(sf133_path=None, force_sf133_load=False, aws_prefix='sf_133', fix_links=True,
                   update_tas_fields=True):
    """ Load any SF-133 files that are not yet in the database and fix any broken links

        Args:
            sf133_path: path to the SF133 files
            force_sf133_load: boolean to indicate whether to force a reload of the data
            aws_prefix: prefix to filter which files to pull from AWS
            fix_links: fix any SF133 records not linked to TAS data
            update_tas_fields: rederive SF133 records if the associated TAS record has been updated
    """
    now = datetime.now()
    metrics_json = {
        'script_name': 'load_sf133.py',
        'start_time': str(now),
        'records_deleted': 0,
        'records_inserted': 0
    }

    with create_app().app_context():
        sess = GlobalDB.db().session

        # get a list of SF 133 files to load
        sf133_list = get_sf133_list(sf133_path, aws_prefix=aws_prefix)
        sf_re = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')
        for sf133 in sf133_list:
            # for each SF file, parse out fiscal year and period and call the SF 133 loader
            file_match = sf_re.match(sf133.file)
            if not file_match:
                logger.info('Skipping SF 133 file with invalid name: %s', sf133.full_file)
                continue
            logger.info('Starting %s...', sf133.full_file)
            load_sf133(sess, sf133.full_file, file_match.group('year'), file_match.group('period'),
                       force_sf133_load=force_sf133_load, metrics=metrics_json)
        if update_tas_fields:
            rederive_tas_fields(sess, metrics=metrics_json)
        if fix_links:
            fix_broken_links(sess, metrics=metrics_json)

    update_external_data_load_date(now, datetime.now(), 'gtas')

    metrics_json['duration'] = str(datetime.now() - now)

    with open('load_sf133_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


def fill_blank_sf133_lines(data):
    """ Incoming .csv does not always include rows for zero-value SF-133 lines so we add those here because they're
        needed for the SF-133 validations.
        1. "pivot" the sf-133 dataset to explode it horizontally, creating one row for each tas/fiscal year/period/defc,
            with columns for each SF-133 line.
        2. Fill any SF-133 line number cells with a missing value for a specific tas/fiscal year/period/defc with a 0.0.
           We don't do this in the "pivot" step because that'll downcast floats to ints
        3. Once the zeroes are filled in, "melt" the pivoted data back to its normal format of one row per tas/fiscal
           year/period/defc.
        NOTE: fields used for the pivot in step #1 (i.e., items in pivot_idx) cannot have NULL values, else they will
        be silently dropped by pandas :(

        Args:
            data: data read in from the files
    """
    pivot_idx = (
        'created_at', 'updated_at', 'agency_identifier', 'allocation_transfer_agency', 'availability_type_code',
        'beginning_period_of_availa', 'ending_period_of_availabil', 'main_account_code', 'sub_account_code', 'tas',
        'fiscal_year', 'period', 'display_tas', 'disaster_emergency_fund_code')

    # The following columns are allowed to be null but still make each row unique
    # For pandas sake, this needs to not be nan and so we're temporarily setting it to something and then fix it later
    nullable_cols = ('disaster_emergency_fund_code')
    temp_value = 'TEMP_NOT_NULL_VALUE'
    data[nullable_cols] = data[nullable_cols].fillna(temp_value)

    data = pd.pivot_table(data, values='amount', index=pivot_idx, columns=['line']).reset_index()
    data = data.fillna(value=0.0)
    data = pd.melt(data, id_vars=pivot_idx, value_name='amount')

    # Reverting the nullable cols back to their original state
    # Setting to empty strings that will be converted to nulls
    data[nullable_cols] = data[nullable_cols].replace(temp_value, '')

    return data


def update_account_num(fiscal_year, fiscal_period, only_broken_links=False):
    """ Set the account_num on newly SF133 entries. We use raw SQL as sqlalchemy doesn't have operators like OVERLAPS
        and IS NOT DISTINCT FROM built in (resulting in a harder to understand query).

        Args:
            fiscal_year: fiscal year to update TAS IDs for
            fiscal_period: fiscal period to update TAS IDs for
            only_broken_links: only update ones with a blank account number
    """
    sess = GlobalDB.db().session
    # number of months since 0AD for this fiscal period
    zero_based_period = fiscal_period - 1
    absolute_fiscal_month = 12 * fiscal_year + zero_based_period - 3
    absolute_following_month = absolute_fiscal_month + 1
    start_date = date(absolute_fiscal_month // 12,
                      (absolute_fiscal_month % 12) + 1,     # 1-based
                      1)
    end_date = date(absolute_following_month // 12,
                    (absolute_following_month % 12) + 1,    # 1-based
                    1)

    subquery = matching_cars_subquery(sess, SF133, start_date, end_date)
    logger.info("Updating account_nums for Fiscal %s-%s", fiscal_year, fiscal_period)
    filters = [SF133.fiscal_year == fiscal_year, SF133.period == fiscal_period]
    if only_broken_links:
        filters.append(SF133.account_num.is_(None))
    sess.query(SF133).\
        filter(*filters).\
        update({SF133.account_num: subquery}, synchronize_session=False)
    sess.commit()


def load_sf133(sess, filename, fiscal_year, fiscal_period, force_sf133_load=False, metrics=None):
    """ Load SF 133 (budget execution report) lookup table.

        Args:
            sess: connection to database
            filename: name/path of the file to read in
            fiscal_year: fiscal year of the file being loaded
            fiscal_period: fiscal period of the file being loaded
            force_sf133_load: boolean to indicate whether to force a reload of the data
            metrics: an object containing information for the metrics file
    """
    if not metrics:
        metrics = {
            'records_inserted': 0,
            'records_deleted': 0
        }

    existing_records = sess.query(SF133).filter(SF133.fiscal_year == fiscal_year, SF133.period == fiscal_period)
    if force_sf133_load:
        # force a reload of this period's current data
        logger.info('Force SF 133 load: deleting existing records for %s %s', fiscal_year, fiscal_period)
        delete_count = existing_records.delete()
        logger.info('%s records deleted', delete_count)
        metrics['records_deleted'] += delete_count
    elif existing_records.count():
        # if there's existing data & we're not forcing a load, skip
        logger.info('SF133 %s %s already in database (%s records). Skipping file.', fiscal_year, fiscal_period,
                    existing_records.count())
        return

    data = clean_sf133_data(filename, SF133)

    # Now that we've added zero lines for EVERY tas and SF 133 line number, get rid of the ones we don't actually
    # use in the validations. Arguably, it would be better just to include everything, but that drastically
    # increases the number of records we're inserting to the sf_133 table. If we ever decide that we need *all*
    # SF 133 lines that are zero value, remove the next two lines.
    sf_133_validation_lines = [
        '1000', '1010', '1011', '1012', '1013', '1020', '1021', '1022', '1023', '1024', '1025', '1026', '1029',
        '1030', '1031', '1032', '1033', '1040', '1041', '1042', '1160', '1180', '1260', '1280', '1340', '1440',
        '1540', '1640', '1750', '1850', '1910', '2190', '2490', '2500', '3020', '4801', '4802', '4881', '4882',
        '4901', '4902', '4908', '4981', '4982'
    ]
    data = data[(data.line.isin(sf_133_validation_lines)) | (data.amount != 0)]

    # we didn't use the the 'keep_null' option when padding allocation transfer agency, because nulls in that column
    # break the pivot (see above comments). so, replace the ata '000' with an empty value before inserting to db
    data['allocation_transfer_agency'] = data['allocation_transfer_agency'].str.replace('000', '')
    # make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as
    # NULL in the db.
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    # Keeping display_tas out here as it depends on empty allocation_transfer_agency being None and not 000
    data['display_tas'] = data.apply(lambda row: concat_display_tas_dict(row), axis=1)

    # insert to db
    table_name = SF133.__table__.name
    num = insert_dataframe(data, table_name, sess.connection())
    metrics['records_inserted'] += num
    update_account_num(int(fiscal_year), int(fiscal_period))
    sess.commit()

    logger.info('%s records inserted to %s', num, table_name)


def clean_sf133_data(filename, sf133_data):
    """ Clean up the data read in from the file.

        Args:
            filename: name/path of the file to be read
            sf133_data: Data model to clean against
    """
    data = pd.read_csv(filename, dtype=str)
    data = clean_data(
        data,
        sf133_data,
        {"ata": "allocation_transfer_agency",
         "aid": "agency_identifier",
         "availability_type_code": "availability_type_code",
         "bpoa": "beginning_period_of_availa",
         "epoa": "ending_period_of_availabil",
         "main_account": "main_account_code",
         "sub_account": "sub_account_code",
         "fiscal_year": "fiscal_year",
         "period": "period",
         "line_num": "line",
         "amount_summed": "amount",
         "defc": "disaster_emergency_fund_code"},
        {"allocation_transfer_agency": {"pad_to_length": 3},
         "agency_identifier": {"pad_to_length": 3},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3},
         # next 3 lines handle the TAS fields that shouldn't be padded but should still be empty spaces rather than
         # NULLs. this ensures that the downstream pivot & melt (which insert the missing 0-value SF-133 lines) will
         # work as expected (values used in the pivot index cannot be NULL). the "pad_to_length: 0" works around the
         # fact that sometimes the incoming data for these columns is a single space and sometimes it is blank/NULL.
         "beginning_period_of_availa": {"pad_to_length": 0},
         "ending_period_of_availabil": {"pad_to_length": 0},
         "availability_type_code": {"pad_to_length": 0},
         "amount": {"strip_commas": True}}
    )

    # todo: find out how to handle dup rows (e.g., same tas/period/line number)
    # line numbers 2002 and 2012 are the only duped SF 133 report line numbers, and they are not used by the validation
    # rules, so for now just remove them before loading our SF-133 table
    dupe_line_numbers = ['2002', '2102']
    data = data[~data.line.isin(dupe_line_numbers)]

    # Uppercasing DEFC to save on indexing
    # Empty values are still empty strings ('') at this point
    data['disaster_emergency_fund_code'] = data['disaster_emergency_fund_code'].str.upper()
    data['disaster_emergency_fund_code'] = data['disaster_emergency_fund_code']. \
        apply(lambda x: x.replace('QQQ', 'Q') if x else None)

    # add concatenated TAS field for internal use (i.e., joining to staging tables)
    data['tas'] = data.apply(lambda row: format_internal_tas(row), axis=1)
    data['display_tas'] = ''
    data['amount'] = data['amount'].astype(float)

    # Grouping by a single column that contains a unique identifier to combine Q/QQQ dupe rows
    data['group_by_col'] = data['tas'] + '_' + data['line'] + '_' + data['disaster_emergency_fund_code']

    data = data.groupby('group_by_col').agg({
        'allocation_transfer_agency': 'max',
        'agency_identifier': 'max',
        'availability_type_code': 'max',
        'beginning_period_of_availa': 'max',
        'ending_period_of_availabil': 'max',
        'main_account_code': 'max',
        'sub_account_code': 'max',
        'fiscal_year': 'max',
        'period': 'max',
        'line': 'max',
        'disaster_emergency_fund_code': 'max',
        'created_at': 'max',
        'updated_at': 'max',
        'tas': 'max',
        'display_tas': 'max',
        'amount': 'sum'
    })

    # Need to round to 2 decimal places now that we've done a sum because floats are weird
    data['amount'] = round(data['amount'], 2)

    data = fill_blank_sf133_lines(data)

    return data


def get_sf133_list(sf133_path, aws_prefix='sf_133'):
    """ Get the list of SF133 files to load

        Args:
            sf133_path: path to where SF133 files are stored
            aws_prefix: prefix to filter which files to pull from AWS

        Returns:
            A list of tuples containing information about existing SF133 files
    """
    SF133File = namedtuple('SF133', ['full_file', 'file'])
    if sf133_path is not None:
        logger.info('Loading local SF-133')
        # get list of SF 133 files in the specified local directory
        sf133_files = glob.glob(os.path.join(sf133_path, 'sf_133*.csv'))
        sf133_list = [SF133File(sf133, os.path.basename(sf133)) for sf133 in sf133_files]
    else:
        logger.info("Loading SF-133")
        if CONFIG_BROKER["use_aws"]:
            # get list of SF 133 files in the config bucket on S3
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            response = s3_client.list_objects_v2(Bucket=CONFIG_BROKER['sf_133_bucket'], Prefix=aws_prefix)
            sf133_list = []
            for obj in response.get('Contents', []):
                if obj['Key'] != 'sf_133':
                    file_url = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                               'Key': obj['Key']},
                                                                ExpiresIn=600)
                    sf133_list.append(SF133File(file_url, obj['Key']))
        else:
            sf133_list = []

    return sf133_list


def rederive_tas_fields(sess, metrics=None):
    """ Using the already derived account_num to update the TAS components for out-of-date SF133 records

        Args:
            sess: connection to the database
            metrics: an object containing information for the metrics file
    """
    if not metrics:
        metrics = {}

    logger.info('Updating any linked SF133 records that are out of date with TAS')
    tas_fields = list(TAS_COMPONENTS) + ['tas', 'display_tas']
    updates = {getattr(SF133, component): getattr(TASLookup, component) for component in tas_fields}
    updated_count = sess.query(SF133).\
        filter(SF133.tas != TASLookup.tas, SF133.account_num == TASLookup.account_num).\
        update(updates, synchronize_session=False)
    sess.commit()

    metrics['updated_tas_fields'] = updated_count
    logger.info('Updated the TAS fields of {} SF133 records'.format(updated_count))


def fix_broken_links(sess, metrics=None):
    """ Simply checks and links any GTAS data currently not linked to TAS data

        Args:
            sess: connection to the database
            metrics: an object containing information for the metrics file
    """
    if not metrics:
        metrics = {}

    logger.info('Updating SF133 data that haven\'t been linked to TAS')
    unlink_count_before = sess.query(SF133).filter(SF133.account_num.is_(None)).count()
    bl_periods = sess.query(SF133.fiscal_year, SF133.period).filter(SF133.account_num.is_(None)).distinct()\
        .order_by(SF133.fiscal_year, SF133.period)
    for fiscal_year, period in bl_periods:
        update_account_num(fiscal_year, period, only_broken_links=True)
    sess.commit()
    unlink_count_after = sess.query(SF133).filter(SF133.account_num.is_(None)).count()

    metrics['fixed_links'] = unlink_count_before - unlink_count_after
    logger.info('Fixed {} broken links to TAS data, {} remain'.format(metrics['fixed_links'], unlink_count_after))


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(description='Initialize the DATA Act Broker.')
    parser.add_argument('-r', '--remote', help='Whether to run remote or not', action='store_true')
    parser.add_argument('-p', '--local_path', help='Local path of folder to check', type=str,
                        default=os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"), required=False)
    parser.add_argument('-pre', '--aws_prefix', help='When loading via AWS, this filters which files to run', type=str,
                        default="sf_133")
    parser.add_argument('-f', '--force', help='Forces actions to occur in certain scripts regardless of checks',
                        action='store_true')
    parser.add_argument('-l', '--fix_links', help='Checks/updates any SF133 data that isn\'t linked to TAS',
                        action='store_true')
    parser.add_argument('-t', '--update_tas_fields', help='Checks/updates any SF133 data with updated TAS data',
                        action='store_true')
    args = parser.parse_args()

    if not args.remote:
        load_all_sf133(args.local_path, args.force, None, args.fix_links, args.update_tas_fields)
    else:
        load_all_sf133(None, args.force, args.aws_prefix, args.fix_links, args.update_tas_fields)
