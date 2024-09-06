import argparse
import json
import logging
import os
import re
import pandas as pd
import numpy as np

from datetime import datetime

from dataactbroker.helpers.generic_helper import format_internal_tas
from dataactbroker.helpers.script_helper import get_prefixed_file_list

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date, exit_if_nonlocal, get_utc_now
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import GTASBOC, concat_display_tas_dict
from dataactcore.utils.loader_utils import insert_dataframe

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def load_all_boc(boc_path=None, force_load=False, aws_prefix='OMB_Extract_BOC'):
    """ Load any GTAS BOC files that are not yet in the database

        Args:
            boc_path: path to the BOC files
            force_load: boolean to indicate whether to force a reload of the data
            aws_prefix: prefix to filter which files to pull from AWS
    """
    now = datetime.now()
    metrics_json = {
        'script_name': 'load_gtas_boc.py',
        'start_time': str(now),
        'records_deleted': 0,
        'records_inserted': 0
    }

    with create_app().app_context():
        sess = GlobalDB.db().session

        boc_list = get_prefixed_file_list(boc_path, aws_prefix, file_extension='txt')
        boc_re = re.compile(r'OMB_Extract_BOC_(?P<year>\d{4})_(?P<period>\d{2})\.txt')
        for boc in boc_list:
            # for each BOC file, parse out fiscal year and period and call the BOC loader
            file_match = boc_re.match(boc.file)
            if not file_match:
                logger.info('Skipping GTAS BOC file with invalid name: %s', boc.full_file)
                continue
            logger.info('Starting %s...', boc.full_file)
            load_boc(sess, boc.full_file, file_match.group('year'), file_match.group('period'),
                     force_load=force_load, metrics=metrics_json)

    update_external_data_load_date(now, datetime.now(), 'gtas_boc')

    metrics_json['duration'] = str(datetime.now() - now)

    with open('load_gtas_boc_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info('BOC loading script complete')

    if metrics_json['records_inserted'] == 0:
        exit_if_nonlocal(3)
        return


def load_boc(sess, filename, fiscal_year, fiscal_period, force_load=False, metrics=None):
    """ Load GTAS BOC lookup table.

        Args:
            sess: connection to database
            filename: name/path of the file to read in
            fiscal_year: fiscal year of the file being loaded
            fiscal_period: fiscal period of the file being loaded
            force_load: boolean to indicate whether to force a reload of the data
            metrics: an object containing information for the metrics file
    """
    if not metrics:
        metrics = {
            'records_inserted': 0,
            'records_deleted': 0
        }

    existing_records = sess.query(GTASBOC).filter(GTASBOC.fiscal_year == fiscal_year, GTASBOC.period == fiscal_period)
    if force_load:
        # force a reload of this period's current data
        logger.info(f'Force GTAS BOC load: deleting existing records for {fiscal_year} {fiscal_period}')
        delete_count = existing_records.delete()
        logger.info(f'{delete_count} records deleted')
        metrics['records_deleted'] += delete_count
    elif existing_records.count():
        # if there's existing data & we're not forcing a load, skip
        logger.info(f'GTAS BOC {fiscal_year} {fiscal_period} already in database ({existing_records.count()} records).'
                    f' Skipping file.')
        return

    boc_col_lengths = {
        'fiscal_year': 4,
        'period': 2,
        'allocation_transfer_agency': 3,
        'agency_identifier': 3,
        'beginning_period_of_availa': 4,
        'ending_period_of_availabil': 4,
        'availability_type_code': 1,
        'main_account_code': 4,
        'sub_account_code': 3,
        'ussgl_number': 6,
        'dollar_amount': 21,
        'debit_credit': 1,
        'begin_end': 1,
        'authority_type': 1,
        'reimbursable_flag': 1,
        'apportionment_cat_code': 1,
        'apportionment_cat_b_prog': 4,
        'program_report_cat_number': 2,
        'federal_nonfederal': 1,
        'trading_partner_agency_ide': 3,
        'trading_partner_mac': 4,
        'year_of_budget_auth_code': 3,
        'availability_time': 1,
        'bea_category': 1,
        'borrowing_source': 1,
        'exchange_or_nonexchange': 1,
        'custodial_noncustodial': 1,
        'budget_impact': 1,
        'prior_year_adjustment_code': 1,
        'credit_cohort_year': 4,
        'disaster_emergency_fund_code': 3,
        'reduction_type': 3,
        'budget_object_class': 4
    }
    boc_col_types = {
        'fiscal_year': int,
        'period': int,
        'allocation_transfer_agency': str,
        'agency_identifier': str,
        'beginning_period_of_availa': str,
        'ending_period_of_availabil': str,
        'availability_type_code': str,
        'main_account_code': str,
        'sub_account_code': str,
        'ussgl_number': str,
        'dollar_amount': float,
        'debit_credit': str,
        'begin_end': str,
        'authority_type': str,
        'reimbursable_flag': str,
        'apportionment_cat_code': str,
        'apportionment_cat_b_prog': str,
        'program_report_cat_number': str,
        'federal_nonfederal': str,
        'trading_partner_agency_ide': str,
        'trading_partner_mac': str,
        'year_of_budget_auth_code': str,
        'availability_time': str,
        'bea_category': str,
        'borrowing_source': str,
        'exchange_or_nonexchange': str,
        'custodial_noncustodial': str,
        'budget_impact': str,
        'prior_year_adjustment_code': str,
        'credit_cohort_year': int,
        'disaster_emergency_fund_code': str,
        'reduction_type': str,
        'budget_object_class': str
    }
    boc_data = pd.read_fwf(filename,
                           widths=boc_col_lengths.values(),
                           names=boc_col_lengths.keys(),
                           converters=boc_col_types)

    boc_data = boc_data.replace({np.nan: None})

    # Drop all rows where availability type code is F or C
    boc_data = boc_data[~boc_data['availability_type_code'].isin(['F', 'C'])]

    boc_data['tas'] = boc_data.apply(lambda row: format_internal_tas(row), axis=1)
    boc_data['display_tas'] = boc_data.apply(lambda row: concat_display_tas_dict(row), axis=1)

    boc_data['disaster_emergency_fund_code'] = boc_data['disaster_emergency_fund_code'].str.upper()
    boc_data['disaster_emergency_fund_code'] = boc_data['disaster_emergency_fund_code']. \
        apply(lambda x: x.replace('QQQ', 'Q') if x else None)

    now = get_utc_now()
    boc_data = boc_data.assign(created_at=now, updated_at=now)

    # insert to db
    table_name = GTASBOC.__table__.name
    num = insert_dataframe(boc_data, table_name, sess.connection())
    metrics['records_inserted'] += num
    sess.commit()

    logger.info(f'{num} records inserted to {table_name}')


def main():
    parser = argparse.ArgumentParser(description='Pull data from the GTAS BOC file.')
    parser.add_argument('-r', '--remote', help='Whether to run remote or not', action='store_true')
    parser.add_argument('-p', '--local_path', help='Local path of folder to check', type=str,
                        default=os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config'), required=False)
    parser.add_argument('-pre', '--aws_prefix', help='When loading via AWS, this filters which files to run', type=str,
                        default='OMB_Extract_BOC')
    parser.add_argument('-f', '--force', help='Forces actions to occur in certain scripts regardless of checks',
                        action='store_true')
    args = parser.parse_args()

    if not args.remote:
        load_all_boc(args.local_path, args.force, args.aws_prefix)
    else:
        load_all_boc(None, args.force, args.aws_prefix)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
