import logging
import argparse

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


# sess: Passing session into the function for modifying db
# type: will be either 'awarding' or 'funding' to determine which agency type we are checking
def update_table(sess, agency_type, args):
    logger.info('updating ' + agency_type)

    # The name of the column depends on the type because of limited string length
    suffix = 'o' if agency_type == "funding" else ''

    invalid_count, condition_sql = set_update_condition(agency_type, suffix, sess, args.subtier_code)

    # updates table based off the parent of the sub_tier_agency code
    sess.execute(
        """
        UPDATE detached_award_procurement SET
        {agency_type}_agency_code = agency.agency_code,
        {agency_type}_agency_name = agency.agency_name FROM (
        SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, CASE WHEN sub.is_frec
        THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id)
        ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) END agency_name,
        CASE WHEN sub.is_frec
        THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id)
        ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) END agency_code
        FROM sub_tier_agency sub
        INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id
        INNER JOIN frec ON frec.frec_id = sub.frec_id ) agency
        WHERE {condition_sql}
        AND detached_award_procurement.{agency_type}_sub_tier_agency_c{suffix} = agency.sub_tier_agency_code;
        """.format(agency_type=agency_type, condition_sql=condition_sql, suffix=suffix)
    )
    sess.commit()
    if args.missing_agency:
        final_invalid_count = get_invalid_count(condition_sql, sess)
        print(final_invalid_count)
        print_report(invalid_count, final_invalid_count, agency_type)


def set_update_condition(agency_type, suffix, sess, subtier_code=None):

    if subtier_code:
        sql_statement = "detached_award_procurement.{}_sub_tier_agency_c{} = '{}' ".format(agency_type,
                                                                                           suffix, subtier_code)
    else:
        sql_statement = "detached_award_procurement." + agency_type + "_agency_code = '999' "

    invalid_count = get_invalid_count(sql_statement, sess)
    logger.info("{} invalid {} rows found".format(invalid_count, agency_type))

    return invalid_count, sql_statement


def get_invalid_count(sql_statement, sess):
    invalid = sess.execute("select count(*) from detached_award_procurement where " + sql_statement + ";")
    invalid_count = invalid.fetchone()[0]

    return invalid_count


# data logging
def print_report(initial, final, type):
    logger.info("{} invalid {} rows updated".format(initial - final, type))
    logger.info("{} invalid {} rows remaining".format(final, type))


def main():
    parser = argparse.ArgumentParser(description='Update contract transaction rows based on updates to the agency list')
    parser.add_argument('-a', '--missing_agency', help='Perform an update on 999 agency codes', action='store_true',
                        required=False, default=False)
    parser.add_argument('-s', '--subtier_code', help='Select specific subtier to update. Must be a 4-digit code',
                        type=str, required=False)
    args = parser.parse_args()

    if not args.subtier_code and not args.missing_agency:
        logger.error('Missing either update_date or missing_agency argument')
    elif args.subtier_code and len(args.subtier_code) != 4:
        logger.error('Subtier not a correct format, must be 4 digits')
    else:
        sess = GlobalDB.db().session

        update_table(sess, 'awarding', args)
        update_table(sess, 'funding', args)
        logger.info("Procurement Update Complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
