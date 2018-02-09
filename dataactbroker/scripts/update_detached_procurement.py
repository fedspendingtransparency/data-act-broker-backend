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
    suffix = 'o' if type == "funding" else ''

    invalid_count, condition = set_update_condition(agency_type, suffix, sess, args.update_date)

    # updates table based off the parent of the sub_tier_agency code
    sess.execute(
        "UPDATE detached_award_procurement SET" +
        + agency_type + "_agency_code = agency.agency_code, " +
        agency_type + "_agency_name = agency.agency_name from ( " +
        "SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, CASE WHEN sub.is_frec " +
        "THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) end agency_name, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) end agency_code " +
        "from sub_tier_agency sub " +
        "INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id " +
        "INNER JOIN frec ON frec.frec_id = sub.frec_id ) agency " +
        "WHERE " + condition +
        "and detached_award_procurement." + agency_type + "_sub_tier_agency_c" + suffix +
        " = agency.sub_tier_agency_code "
    )
    # sess.commit()
    if args.missing_agency:
        invalid = sess.execute("select count(*) from" + condition)
        print_report(invalid_count, invalid.fetchone()[0], type)


def set_update_condition(agency_type, suffix, sess, update_date=None):

    if update_date:
        updated_subtiers = sess.execute(
            "SELECT sub_tier_agency_code from sub_tier_agency where updated_at::date='{}'::date".format(update_date)
        )
        subtier_list = ",".join(["\'{}\'".format(str(x[0])) for x in updated_subtiers.fetchall() if len(x[0]) == 4])
        sql_statement = "detached_award_procurement."+agency_type+"_sub_tier_agency_c"+suffix+" in ("+subtier_list+")"
    else:
        sql_statement = "detached_award_procurement." + agency_type + "_agency_code = '999' "

    invalid = sess.execute("select count(*) from " + sql_statement)
    invalid_count = invalid.fetchone()[0]
    logger.info("{} invalid {} rows found".format(invalid_count, agency_type))

    return invalid_count, sql_statement


# data logging
def print_report(initial, final, type):
    logger.info("{} invalid {} rows updated".format(initial - final, type))
    logger.info("{} invalid {} rows remaining".format(final, type))


def main():
    parser = argparse.ArgumentParser(description='Update contract transaction rows based on updates to the agency list')
    parser.add_argument('--missing_agency', help='Perform an update on 999 agency codes', action='store_true',
                        required=False, default=False)
    parser.add_argument('-d', '--update_date', help='Date subtier were updated at to run updates on the' +
                                                    'toptier agencies derived in transactions. Format: YYYY/MM/DD',
                        type=str, required=False)
    args = parser.parse_args()

    if not args.update_date and not args.missing_agency:
        logger.error('Missing either update_date or missing_agency argument')
    else:
        sess = GlobalDB.db().session

        update_table(sess, 'awarding', args)
        update_table(sess, 'funding', args)
        logger.info("Procurement Update Complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
