import logging
import argparse
from sqlalchemy import and_, or_
import pandas as pd

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app

from dataactbroker.helpers.generic_helper import batch
from dataactcore.utils.duns import update_duns_props, update_duns, LOAD_BATCH_SIZE
from dataactcore.models.domainModels import DUNS

logger = logging.getLogger(__name__)


def backfill_uei_via_entity_api(sess, table):
    """ Backfill any extraneous data (ex. uei) missing from V1 data that wasn't updated by V2

        Args:
            sess: database connection
            table: table to backfill
    """
    duns_to_update = sess.query(table.awardee_or_recipient_uniqu).filter(
        or_(DUNS.uei.is_(None), and_(DUNS.ultimate_parent_unique_ide.isnot_(None),
                                     DUNS.ultimate_parent_uei.is_(None)))).all()
    for duns_batch in batch(duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame(columns=['awardee_or_recipient_uniqu'])
        df = df.append(duns_batch)
        df = update_duns_props(df)
        df = df[['awardee_or_recipient_uniqu', 'uei', 'ultimate_parent_uei']]
        update_duns(sess, df, table_name=table.__table__.name)


def backfill_uei_crosswalk(sess, table_name):
    """ Backfill any extraneous data (ex. uei) missing from V1 data that wasn't updated by V2

        Args:
            sess: database connection
            table_name: table to backfill
    """
    blank_uei_query = """
        SELECT awardee_or_recipient_uniqu
        FROM {table_name}
        WHERE uei IS NULL;
    """.format(table_name=table_name)
    duns_to_update = [row['awardee_or_recipient_uniqu'] for row in sess.execute(blank_uei_query).fetchall()]
    for duns_batch in batch(duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame()
        df['awardee_or_recipient_uniqu'] = duns_batch
        df = update_duns_props(df, api='iqaas')
        df = df[['awardee_or_recipient_uniqu', 'uei']]
        update_duns(sess, df, table_name=table_name)


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Get data from SAM and backfill uei')
    parser.add_argument("-m", "--method", choices=['duns', 'crosswalk'], default='crosswalk',
                        help='Select method of backfilling (duns table, uei crosswalk table)')
    parser.add_argument("-ct", "--crosswalk_table", default='uei-crosswalk',
                        help='Name of the crosswalk table to backfill')
    return parser


if __name__ == '__main__':
    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    method = args.method
    crosswalk_table = args.crosswalk_table

    with create_app().app_context():
        sess = GlobalDB.db().session

        affected = 0

        if method == 'duns':
            logger.info('Backfilling empty uei and ultimate_parent_uei in the DUNS table using the entity API.')
            backfill_uei_via_entity_api(sess, DUNS)
        else:
            logger.info('Backfilling {} using the IQaaS API.'.format(crosswalk_table))
            backfill_uei_crosswalk(sess, table_name=crosswalk_table)
        logger.info('Backfill completed')

        sess.close()
