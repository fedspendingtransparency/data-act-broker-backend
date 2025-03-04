import os
import argparse
import datetime
import logging
import json

from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.broker_logging import configure_logging
from dataactvalidator.health_check import create_app

RAW_SQL_DIR = os.path.join(CONFIG_BROKER['path'], 'dataactcore', 'scripts', 'raw_sql')

POPULATE_CONTRACT_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_contract.sql')
POPULATE_ASSISTANCE_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_assistance.sql')
LINK_CONTRACT_SQL = os.path.join(RAW_SQL_DIR, 'link_broken_subaward_contract.sql')
LINK_ASSISTANCE_SQL = os.path.join(RAW_SQL_DIR, 'link_broken_subaward_assistance.sql')

logger = logging.getLogger(__name__)


def extract_subaward_sql(data_type, data_change_type):
    """ Gather the SAM subaward SQL requested

        Args:
            data_type: type of service to ping ('contract' or 'assistance')
            data_change_type: type of data change involving subawards ('populate' or 'link')
        Returns:
            sql to run based on the request
        Raises:
            Exception: service type is invalid
            Exception: data change type is invalid
    """
    pop_sql_map = {'contract': POPULATE_CONTRACT_SQL, 'assistance': POPULATE_ASSISTANCE_SQL}
    link_sql_map = {'contract': LINK_CONTRACT_SQL, 'assistance': LINK_ASSISTANCE_SQL}
    if data_type not in pop_sql_map:
        raise Exception('Invalid data type provided: {}'.format(data_type))
    type_map = {'populate': pop_sql_map, 'link': link_sql_map}
    if data_change_type not in type_map:
        raise Exception('Invalid data change type provided: {}'.format(data_change_type))
    with open(type_map[data_change_type][data_type], 'r') as sql_file:
        sql = sql_file.read()
    return sql


def populate_subaward_table(sess, data_type, min_date=None, report_nums=None):
    """ Populates the subaward table based on the IDS (or min id) provided

        Args:
            sess: connection to the database
            data_type: type of data to work with (usually 'contract' or 'assistance')
            min_date: the earliest updated_at to use from the sam subaward tables
            report_nums: if provided, only update these ids

        Raises:
            Exception: data type is invalid
    """
    sql = extract_subaward_sql(data_type, 'populate')
    table_name = 'sam_subgrant' if data_type == 'assistance' else 'sam_subcontract'
    if min_date is not None:
        condition = f'{table_name}.updated_at > \'{min_date.strftime("%Y-%m-%d")}\''
    else:
        report_nums = ','.join([str(report_num) for report_num in report_nums])
        condition = f'{table_name}.subaward_report_id IN ({report_nums})'
    sql = sql.format(condition)

    # run the SQL. splitting and stripping the calls for pg_stat_activity visibility while it's running
    for sql_statement in sql.split(';'):
        if sql_statement.strip():
            inserted = sess.execute(sql_statement.strip())
    sess.commit()
    inserted_count = inserted.rowcount
    logger.info(f'Inserted {inserted_count} sub-{data_type} to the subaward table')
    return inserted_count


# TODO
def fix_broken_links(sess, data_type):
    """ Attempts to resolve any unlinked subawards given the current data

        Args:
            sess: connection to the database
            data_type: type of data to work with (usually 'contract' or 'assistance')

        Raises:
            Exception: data type is invalid
    """
    logger.info(f'Attempting to fix broken sub-{data_type} links in the subaward table')

    sql = extract_subaward_sql(data_type, 'link')

    # run the SQL. splitting and stripping the calls for pg_stat_activity visibility while it's running
    for sql_statement in sql.split(';'):
        if sql_statement.strip():
            updated = sess.execute(sql_statement.strip())
    sess.commit()

    updated_count = updated.rowcount
    logger.info(f'Updated {updated_count} sub-{data_type} in the subaward table')
    return updated_count


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Populate the subaward table using the raw SAM Subaward tables')
    method = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-d', '--data_type', help='Which data (assistance, contract, or both) to populate.'
                                                  ' Defaults to both.',
                        required=False, default='both', choices=['assistance', 'contract', 'both'])
    method.add_argument('-m', '--min_date', type=str, nargs=1, help="Load all data from a minimum date"
                                                                    " (mm/dd/yyyy) using 'updated_at'")
    method.add_argument('-i', '--ids', type=int, nargs='+',
                        help="Single or list of SAM Subaward Report Numbers to populate the subaward table")

    with create_app().app_context():
        logger.info("Begin backfilling Subaward table")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        metrics_json = {
            'script_name': 'populate_subaward_table.py',
            'records_inserted': 0,
            'start_time': str(now)
        }

        min_date = None
        if args.min_date:
            min_date = datetime.datetime.strptime(args.min_date[0], '%m/%d/%Y')

        records_inserted = 0
        data_types = ['contract', 'assistance'] if args.data_type == 'both' else [args.data_type]
        for data_type in data_types:
            if args.min_date:
                records_inserted += populate_subaward_table(sess, data_type, min_date=min_date)
            elif args.ids:
                records_inserted += populate_subaward_table(sess, data_type, report_nums=args.ids)

        metrics_json['records_inserted'] = records_inserted
        metrics_json['duration'] = str(datetime.datetime.now() - now)

        with open('populate_subaward_table  .json', 'w+') as metrics_file:
            json.dump(metrics_json, metrics_file)
