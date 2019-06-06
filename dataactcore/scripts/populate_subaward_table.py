import os
import argparse
import datetime
import logging
import json
import sys

from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactbroker.fsrs import GRANT, PROCUREMENT

RAW_SQL_DIR = os.path.join(CONFIG_BROKER['path'], 'dataactcore', 'scripts', 'raw_sql')
POPULATE_PROCUREMENT_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_contracts.sql')
POPULATE_GRANT_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_grants.sql')
LINK_PROCUREMENT_SQL = os.path.join(RAW_SQL_DIR, 'link_broken_subaward_contracts.sql')
LINK_GRANT_SQL = os.path.join(RAW_SQL_DIR, 'link_broken_subaward_grants.sql')

logger = logging.getLogger(__name__)


def extract_subaward_sql(service_type, data_change_type):
    """ Gather the subaward SQL requested

        Args:
            service_type: type of service to ping ('procurement_service' or 'grant_service')
            data_change_type: type of data change involving subawards ('populate' or 'link')

        Raises:
            Exception: service type is invalid
            Exception: data change type is invalid
    """
    pop_sql_map = {PROCUREMENT: POPULATE_PROCUREMENT_SQL, GRANT: POPULATE_GRANT_SQL}
    link_sql_map = {PROCUREMENT: LINK_PROCUREMENT_SQL, GRANT: LINK_GRANT_SQL}
    if service_type not in pop_sql_map:
        raise Exception('Invalid service type provided: {}'.format(service_type))
    type_map = {'populate': pop_sql_map, 'link': link_sql_map}
    if data_change_type not in type_map:
        raise Exception('Invalid data change type provided: {}'.format(data_change_type))
    with open(type_map[data_change_type][service_type], 'r') as sql_file:
        sql = sql_file.read()
    return sql


def populate_subaward_table(sess, service_type, ids=None, min_id=None):
    """ Populates the subaward table based on the IDS (or min id) provided

        Args:
            sess: connection to the database
            service_type: type of service to ping (usually 'procurement_service' or 'grant_service')
            ids: if provided, only update these ids
            min_id: if provided, update all ids past this one

        Raises:
            Exception: ids and min_id both provided or both not provided
            Exception: service type is invalid
    """
    if (ids is not None and min_id is not None) or (ids is None and min_id is None):
        raise Exception('ids or min_id, but not both, must be provided')

    sql = extract_subaward_sql(service_type, 'populate')
    if min_id is not None:
        operator = '>'
        values = min_id - 1
    else:
        operator = 'IN'
        values = '({})'.format(','.join([str(id) for id in ids]))
    sql = sql.format(operator, values)

    # run the SQL
    inserted = sess.execute(sql)
    sess.commit()
    inserted_count = inserted.rowcount
    award_type = service_type[:service_type.index('_')]
    logger.info('Inserted {} sub-{}s to the subaward table'.format(inserted_count, award_type))
    return inserted_count


def fix_broken_links(sess, service_type, min_date=None):
    """ Attempts to resolve any unlinked subawards given the current data

        Args:
            sess: connection to the database
            service_type: type of service to ping (usually 'procurement_service' or 'grant_service')

        Raises:
            Exception: service type is invalid
    """
    award_type = service_type[:service_type.index('_')]
    logger.info('Attempting to fix broken sub-{} links in the subaward table'.format(award_type))
    subaward_type_map = {PROCUREMENT: 'sub-contract', GRANT: 'sub-grant'}
    if service_type not in subaward_type_map:
        raise Exception('Invalid service type provided: {}'.format(service_type))

    sql = extract_subaward_sql(service_type, 'link')
    min_date_sql = '' if min_date is None else 'AND updated_at >= \'{}\''.format(min_date)
    sql = sql.format(min_date_sql)

    # run the SQL
    updated = sess.execute(sql)
    sess.commit()

    updated_count = updated.rowcount
    logger.info('Updated {} sub-{}s in the subaward table'.format(updated_count, award_type))
    return updated_count


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Pull data from FSRS Feed')
    method = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-p', '--procurements', action='store_true', help="Load just procurement awards")
    parser.add_argument('-g', '--grants', action='store_true', help="Load just grant awards")
    method.add_argument('-m', '--min_id', type=int, nargs=1, help="Load all data from a minimum id (0 for complete"
                                                                  " backfill)")
    method.add_argument('-i', '--ids', type=int, nargs='+',
                        help="Single or list of FSRS ids to populate the subaward table")

    with create_app().app_context():
        logger.info("Begin backfilling Subaward table")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        metrics_json = {
            'script_name': 'populate_subaward_table.py',
            'records_inserted': 0,
            'start_time': str(now)
        }

        service_types = []
        if not (args.procurements or args.grants):
            logger.error('FSRS types not provided. Please specify procurements, grants, or both.')
            sys.exit(1)
        if args.procurements:
            service_types.append(PROCUREMENT)
        if args.grants:
            service_types.append(GRANT)

        records_inserted = 0
        for service_type in service_types:
            if args.min_id:
                records_inserted += populate_subaward_table(sess, service_type, min_id=args.min_id[0])
            elif args.ids:
                records_inserted += populate_subaward_table(sess, service_type, ids=args.ids)

        metrics_json['records_inserted'] = records_inserted
        metrics_json['duration'] = str(datetime.datetime.now() - now)

        with open('populate_subaward_table  .json', 'w+') as metrics_file:
            json.dump(metrics_json, metrics_file)
