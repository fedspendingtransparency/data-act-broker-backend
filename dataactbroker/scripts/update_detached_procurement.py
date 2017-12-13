import logging

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_table(sess, table_type):
    logger.info('updating ' + table_type)
    invalid = sess.execute("select * from detached_award_procurement where " + table_type + "_agency_code='999'")
    invalid_count = len(invalid.fetchall())
    logger.info("{} invalid {} rows found".format(invalid_count, table_type))
    suffix = 'o' if table_type == "funding" else ''
    sess.execute(
        "UPDATE detached_award_procurement set " + table_type + "_agency_code = agency.agency_code, " +
        table_type + "_agency_name = agency.agency_name from ( " +
        "SELECT sub.sub_tier_agency_code, sub.cgac_id, sub.frec_id, sub.is_frec, CASE WHEN sub.is_frec " +
        "THEN (SELECT agency_name from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT agency_name from cgac where cgac.cgac_id = sub.cgac_id) end agency_name, " +
        "CASE WHEN sub.is_frec " +
        "THEN (SELECT frec_code from frec WHERE frec.frec_id = sub.frec_id) " +
        "ELSE (SELECT cgac_code from cgac where cgac.cgac_id = sub.cgac_id) end agency_code " +
        "from sub_tier_agency sub " +
        "INNER JOIN cgac ON cgac.cgac_id = sub.cgac_id " +
        "INNER JOIN frec ON frec.frec_id = sub.frec_id ) agency " +
        "where detached_award_procurement." + table_type + "_agency_code = '999' " +
        "and detached_award_procurement." + table_type + "_sub_tier_agency_c" + suffix +
        " = agency.sub_tier_agency_code "
    )
    sess.commit()
    invalid = sess.execute("select * from detached_award_procurement where " + table_type + "_agency_code='999'")
    print_report(invalid_count, len(invalid.fetchall()), table_type)


def print_report(initial, final, table_type):
    logger.info("{} invalid {} rows removed".format(initial - final, table_type))
    logger.info("{} invalid {} rows remaining".format(final, table_type))


def main():
    sess = GlobalDB.db().session

    update_table(sess, 'awarding')
    update_table(sess, 'funding')
    logger.info("Procurement Update Complete")

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
