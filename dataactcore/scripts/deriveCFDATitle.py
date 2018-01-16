import logging
from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance
from dataactcore.models.domainModels import CFDAProgram

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_cfda(sess):
    cfda = sess.query(CFDAProgram).all()
    cfda_list = {}
    for item in cfda:
        cfda_list[str('%.3f' % item.program_number)] = item.program_title

    update_list = sess.query(PublishedAwardFinancialAssistance.cfda_number).\
        filter(PublishedAwardFinancialAssistance.cfda_title.is_(None)).\
        group_by(PublishedAwardFinancialAssistance.cfda_number)
    cfda_count = 0

    for row in update_list:
        cfda_query = sess.query(PublishedAwardFinancialAssistance).\
            filter(
                    PublishedAwardFinancialAssistance.cfda_title.is_(None),
                    PublishedAwardFinancialAssistance.cfda_number == row.cfda_number).\
            update({"cfda_title": cfda_list[row.cfda_number]},
                   synchronize_session=False)
        logger.info('%s entries with CFDA number %s have been updated with title "%s"',
                    cfda_query, row.cfda_number, cfda_list[row.cfda_number])
        cfda_count += 1
    logger.info('%s PublishedAwardFinancialAssistance records updated', cfda_count)
    sess.commit()


def main():
    sess = GlobalDB.db().session
    logger.info('CFDA update started')
    update_cfda(sess)
    logger.info("CFDA update finished")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
