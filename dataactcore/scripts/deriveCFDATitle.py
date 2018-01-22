import logging
import datetime
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

    logger.info('Gathering CFDA numbers with null titles')

    # Grabs list of all cfda_numbers
    update_list = sess.query(PublishedAwardFinancialAssistance.cfda_number).\
        filter(PublishedAwardFinancialAssistance.cfda_title.is_(None))

    count = update_list.count()
    total = count
    complete = 0

    update_list = update_list.group_by(PublishedAwardFinancialAssistance.cfda_number)

    logger.info('%s null CFDA titles found %s', count, [row[0] for row in update_list.all()])

    cfda_count = 0
    for row in update_list:
        start_time = datetime.datetime.now()
        current_cfda = row.cfda_number
        cfda_query = sess.query(PublishedAwardFinancialAssistance).\
            filter(
                    PublishedAwardFinancialAssistance.cfda_title.is_(None),
                    PublishedAwardFinancialAssistance.cfda_number == row.cfda_number)
        if len(current_cfda.split('.')) < 2:
            logger.info('CFDA_number %s is not a valid CFDA_number', current_cfda)
            invalid_count = cfda_query.count()
            count = count - invalid_count
            complete = complete + invalid_count
            logger.info('%s entries are invalid', invalid_count)
            continue
        # auto pad to get to length 6 because of how we are holding the cfda numbers
        while len(current_cfda.split('.')[1]) < 3:
            current_cfda = "%s%s" % (current_cfda, '0')
        if current_cfda not in cfda_list:
            logger.info('CFDA_number %s is not a valid CFDA_number', current_cfda)
            invalid_count = cfda_query.count()
            count = count - invalid_count
            complete = complete + invalid_count
            logger.info('%s entries are invalid', invalid_count)
            continue
        cfda_query = cfda_query.update({"cfda_title": cfda_list[current_cfda]},
                                       synchronize_session=False)

        count = count - cfda_query
        complete = complete + cfda_query
        end_time = datetime.datetime.now()
        logger.info('%s entries updated, %s entries remaining, %s percent complete',
                    complete, count, (complete/total)*100)
        logger.info('CFDA %s took %s to complete', row.cfda_number, end_time - start_time)
        logger.info('%s entries with CFDA number %s have been updated with title "%s"',
                    cfda_query, row.cfda_number, cfda_list[current_cfda])
        cfda_count += 1
    logger.info('%s PublishedAwardFinancialAssistance records updated', cfda_count)
    sess.commit()


def main():
    start = datetime.datetime.now()
    sess = GlobalDB.db().session
    logger.info('CFDA update started')
    update_cfda(sess)
    end = datetime.datetime.now()
    logger.info('CFDA update finished in %s seconds', end-start)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
