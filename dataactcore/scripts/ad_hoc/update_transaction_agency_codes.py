import logging
import argparse

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_table(sess, agency_type, table, args):
    """ Update the given table's agency codes. If provided, only update a specific sub tier code, otherwise update
        any entry with a 999 in the agency code (awarding and funding separately).

        Args:
            sess: the database connection
            agency_type: string indicating what agency type we're checking, either 'funding' or 'awarding'
            table: string indicating which table to update, either 'detached_award_procurement' or
                'published_fabs'
            args: object containing the arguments provided by the user
    """
    # Setting the type of update we are running on the procurement table for logging purposes
    update_type = {'level': 'cgac', 'codes': ['999']} if args.missing_agency \
        else {'level': 'subtier', 'codes': args.subtier_codes}

    logger.info('Updating ' + agency_type + ' ' + update_type['level'] + ' agency codes ' + str(update_type['codes'])
                + ' for ' + table + ' table')

    # The name of the column depends on the type because of limited string length
    suffix = 'o' if agency_type == "funding" else ''

    row_count, condition_sql = set_update_condition(agency_type, table, suffix, sess, args.subtier_codes)

    # updates table based off the parent of the sub_tier_agency code
    sess.execute(
        """
        UPDATE {table_name}
        SET
            {agency_type}_agency_code = agency.agency_code,
            {agency_type}_agency_name = agency.agency_name
        FROM (
            SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec,
                CASE WHEN sub.is_frec
                    THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id)
                    ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) END agency_name,
                CASE WHEN sub.is_frec
                    THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id)
                    ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) END agency_code
            FROM sub_tier_agency sub
            INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id
            INNER JOIN frec ON frec.frec_id = sub.frec_id ) agency
        WHERE {condition_sql}
            AND {table_name}.{agency_type}_sub_tier_agency_c{suffix} = agency.sub_tier_agency_code;
        """.format(table_name=table, agency_type=agency_type, condition_sql=condition_sql, suffix=suffix)
    )
    sess.commit()

    final_row_count = 0 if args.subtier_codes else get_row_count(condition_sql, table, sess)
    print_report(row_count, final_row_count, agency_type, True)

    if args.missing_agency:
        logger.info("{} agency code 999 remaining: {} rows ".format(agency_type.title(), final_row_count))


def set_update_condition(agency_type, table, suffix, sess, subtier_codes=None):
    """ Changes the condition on which to update based on the type of update (999 vs subtier code).

        Args:
            agency_type: string indicating what agency type we're checking, either 'funding' or 'awarding'
            table: string indicating which table to update, either 'detached_award_procurement' or
                'published_fabs'
            suffix: string to add an 'o' to a column name if it's a funding agency
            sess: the database connection
            subtier_codes: the list of sub tier codes to update if provided, otherwise None

        Returns:
            The row count for the number of rows that match the query and "WHERE" condition created
    """
    if subtier_codes and len(subtier_codes) == 1:
        sql_statement = "{}.{}_sub_tier_agency_c{} = '{}' ".format(table, agency_type, suffix, str(subtier_codes[0]))
    elif subtier_codes and len(subtier_codes) > 1:
        subtiers_str = '({})'.format(','.join(['\'{}\''.format(subtier_code) for subtier_code in subtier_codes]))
        sql_statement = "{}.{}_sub_tier_agency_c{} IN {}".format(table, agency_type, suffix, subtiers_str)
    else:
        sql_statement = "{}.{}_agency_code = '999' ".format(table, agency_type)

    row_count = get_row_count(sql_statement, table, sess)
    print_report(row_count, 0, agency_type)

    return row_count, sql_statement


def get_row_count(sql_statement, table, sess):
    """ Runs a SQL query to get the count of transaction rows based on the type of update. SQL Statement will either be
        agency_code = '999' or sub_tier_agency_code = 'XXXX' for awarding and funding.

        Args:
            sql_statement: string containing the WHERE statement for the query
            table: string indicating which table to count from, either 'detached_award_procurement' or
                'published_fabs'
            sess: the database connection

        Returns:
            The count of how many rows in the provided table match the provided WHERE statement
    """
    rows = sess.execute("SELECT COUNT(*) FROM " + table + " WHERE " + sql_statement + ";")
    row_count = rows.fetchone()[0]

    return row_count


def print_report(initial, final, agency_type, is_updated=False):
    """ Logs row count before and after and update.

        Args:
            initial: The total number of rows before the updates
            final: The number of rows remaining after the fix (0 if being logged before update)
            agency_type: string indicating what agency type we're checking, either 'funding' or 'awarding'
            is_updated: boolean indicating if this print is for before or after the update
    """
    logger.info("{} codes {}: {} rows".format(agency_type.title(), 'updated' if is_updated else 'to update',
                                              initial - final))


def main():
    parser = argparse.ArgumentParser(description='Update contract transaction and/or fabs rows based on updates to the '
                                                 'agency list')
    parser.add_argument('-a', '--missing_agency', help='Perform an update on 999 agency codes', action='store_true',
                        required=False, default=False)
    parser.add_argument('-s', '--subtier_codes', help='Select specific subtiers to update. Must be a 4-digit code',
                        type=str, nargs='+', required=False)
    parser.add_argument('-t', '--tables', help='Which tables (fabs, fpds, or both) to update. Defaults to both.',
                        required=False, default='both', choices=['fabs', 'fpds', 'both'])
    args = parser.parse_args()

    if not args.subtier_codes and not args.missing_agency:
        logger.error('Missing either subtier_code or missing_agency argument')
        return
    if args.subtier_codes:
        for subtier_code in args.subtier_codes:
            if len(subtier_code) != 4:
                logger.error('Subtier not a correct format, must be 4 digits')
                return

    sess = GlobalDB.db().session

    if args.tables in ('fpds', 'both'):
        update_table(sess, 'awarding', 'detached_award_procurement', args)
        update_table(sess, 'funding', 'detached_award_procurement', args)
        logger.info("Procurement Update Complete")

    if args.tables in ('fabs', 'both'):
        update_table(sess, 'awarding', 'published_fabs', args)
        update_table(sess, 'funding', 'published_fabs', args)
        logger.info("Award Financial Assistance Update Complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
