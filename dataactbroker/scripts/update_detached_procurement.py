import logging
import datetime

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


# sess: Passing session into the function for modifying db
# type: will be either 'awarding' or 'funding' to determine which agency type we are checking
def update_table(sess, type):
    logger.info('updating ' + type)
    invalid = sess.execute("select count(*) from detached_award_procurement where " + type + "_agency_code='999'")
    invalid_count = invalid.fetchone()[0]
    logger.info("{} invalid {} rows found".format(invalid_count, type))
    # The name of the column depends on the type because of limited string length
    suffix = 'o' if type == "funding" else ''

    updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %I:%M:%S.%f')
    # updates table based off the parent of the sub_tier_agency code
    sess.execute(
        "UPDATE detached_award_procurement set updated_at = '" + updated_at + "', "
        + type + "_agency_code = agency.agency_code, " +
        type + "_agency_name = agency.agency_name from ( " +
        "SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, CASE WHEN sub.is_frec " +
        "THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) end agency_name, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) end agency_code " +
        "from sub_tier_agency sub " +
        "INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id " +
        "INNER JOIN frec ON frec.frec_id = sub.frec_id ) agency " +
        "where detached_award_procurement." + type + "_agency_code = '999' " +
        "and detached_award_procurement." + type + "_sub_tier_agency_c" + suffix +
        " = agency.sub_tier_agency_code "
    )
    sess.commit()
    invalid = sess.execute("select count(*) from detached_award_procurement where " + type + "_agency_code='999'")
    print_report(invalid_count, invalid.fetchone()[0], type)


# data logging
def print_report(initial, final, type):
    logger.info("{} invalid {} rows updated".format(initial - final, type))
    logger.info("{} invalid {} rows remaining".format(final, type))


def main():
    sess = GlobalDB.db().session

    update_table(sess, 'awarding')
    update_table(sess, 'funding')
    logger.info("Procurement Update Complete")

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
