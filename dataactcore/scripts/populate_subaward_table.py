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

RAW_SQL_DIR = os.path.join(CONFIG_BROKER['path'], 'dataactcore', 'scripts', 'raw_sql')
PROCUREMENT_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_contracts.sql')
GRANT_SQL = os.path.join(RAW_SQL_DIR, 'populate_subaward_table_grants.sql')

logger = logging.getLogger(__name__)

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
        raise Exception('ids or min_id must be provided')

    # Gather the populate subaward SQL
    if service_type == 'procurements':
        sql_file_path = PROCUREMENT_SQL
    elif service_type == 'grants':
        sql_file_path = GRANT_SQL
    else:
        raise Exception('Invalid service type provided: {}'.format(service_type))
    with open(sql_file_path, 'r') as sql_file:
        sql = sql_file.read()
    if min_id is not None:
        operator = '>'
        values = min_id
    else:
        operator = 'IN'
        values = '({})'.format(','.join([str(id) for id in ids]))
    sql = sql.format(operator, values)

    # run the SQL
    inserted = sess.execute(sql)
    sess.commit()
    inserted_count = inserted.rowcount
    logger.info('Inserted {} sub-{} to the subaward table'.format(inserted_count, service_type))
    return inserted_count

if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Pull data from FSRS Feed')
    method = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-p', '--procurements', action='store_true', help="Load just procurement awards")
    parser.add_argument('-g', '--grants', action='store_true', help="Load just grant awards")
    method.add_argument('-b', '--backfill', action='store_true', help="Backfill")
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
            service_types.append('procurements')
        if args.grants:
            service_types.append('grants')

        records_inserted = 0
        for service_type in service_types:
            if args.backfill:
                records_inserted += populate_subaward_table(sess, service_type, min_id=0)
            elif args.ids:
                records_inserted += populate_subaward_table(sess, service_type, ids=args.ids)

        metrics_json['records_inserted'] = records_inserted
        metrics_json['duration'] = str(datetime.datetime.now() - now)

        with open('populate_subaward_table  .json', 'w+') as metrics_file:
            json.dump(metrics_json, metrics_file)
