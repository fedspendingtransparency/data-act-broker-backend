import os
import argparse
import datetime
import logging
import json
import sys

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
    if service_type not in pop_sql_map:
        raise Exception('Invalid data type provided: {}'.format(data_type))
    type_map = {'populate': pop_sql_map, 'link': link_sql_map}
    if data_change_type not in type_map:
        raise Exception('Invalid data change type provided: {}'.format(data_change_type))
    with open(type_map[data_change_type][service_type], 'r') as sql_file:
        sql = sql_file.read()
    return sql


def populate_subaward_table(sess, data_type, min_date, report_nums):
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
        condition = f'{table_name}.updated_at > {min_date.strftime("%Y-%m-%d")}'
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
def fix_broken_links(sess, data_type, min_date=None):
    """ Attempts to resolve any unlinked subawards given the current data

        Args:
            sess: connection to the database
            data_type: type of data to work with (usually 'contract' or 'assistance')
            min_date: the earliest updated_at to use from the sam subaward tables

        Raises:
            Exception: data type is invalid
    """
    logger.info(f'Attempting to fix broken sub-{data_type} links in the subaward table')

    sql = extract_subaward_sql(data_type, 'link')
    min_date_sql = '' if min_date is None else 'AND updated_at >= \'{}\''.format(min_date)
    sql = sql.format(min_date_sql)

    # run the SQL. splitting and stripping the calls for pg_stat_activity visibility while it's running
    for sql_statement in sql.split(';'):
        if sql_statement.strip():
            updated = sess.execute(sql_statement.strip())
    sess.commit()

    updated_count = updated.rowcount
    logger.info(f'Updated {updated_count} sub-{data_type} in the subaward table')
    return updated_count


# TODO: rework based on sam version
if __name__ == '__main__':
    pass
    # now = datetime.datetime.now()
    # configure_logging()
    # parser = argparse.ArgumentParser(description='Pull data from FSRS Feed')
    # method = parser.add_mutually_exclusive_group(required=True)
    # parser.add_argument('-p', '--procurements', action='store_true', help="Load just procurement awards")
    # parser.add_argument('-g', '--grants', action='store_true', help="Load just grant awards")
    # method.add_argument('-m', '--min_id', type=int, nargs=1, help="Load all data from a minimum id (0 for complete"
    #                                                               " backfill)")
    # method.add_argument('-i', '--ids', type=int, nargs='+',
    #                     help="Single or list of FSRS ids to populate the subaward table")
    #
    # with create_app().app_context():
    #     logger.info("Begin backfilling Subaward table")
    #     sess = GlobalDB.db().session
    #     args = parser.parse_args()
    #
    #     metrics_json = {
    #         'script_name': 'populate_subaward_table.py',
    #         'records_inserted': 0,
    #         'start_time': str(now)
    #     }
    #
    #     service_types = []
    #     if not (args.procurements or args.grants):
    #         logger.error('FSRS types not provided. Please specify procurements, grants, or both.')
    #         sys.exit(1)
    #     if args.procurements:
    #         service_types.append(PROCUREMENT)
    #     if args.grants:
    #         service_types.append(GRANT)
    #
    #     records_inserted = 0
    #     for service_type in service_types:
    #         if args.min_id:
    #             records_inserted += populate_subaward_table(sess, service_type, min_id=args.min_id[0])
    #         elif args.ids:
    #             records_inserted += populate_subaward_table(sess, service_type, ids=args.ids)
    #
    #     metrics_json['records_inserted'] = records_inserted
    #     metrics_json['duration'] = str(datetime.datetime.now() - now)
    #
    #     with open('populate_subaward_table  .json', 'w+') as metrics_file:
    #         json.dump(metrics_json, metrics_file)
