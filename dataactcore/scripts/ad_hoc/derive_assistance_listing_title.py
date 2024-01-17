import logging
import datetime
from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import PublishedFABS
from dataactcore.models.domainModels import AssistanceListing

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_assistance_listing(sess):
    assistance_listing = sess.query(AssistanceListing).all()
    assistance_listing_list = {}
    for item in assistance_listing:
        assistance_listing_list[str('%.3f' % item.program_number)] = item.program_title

    logger.info('Gathering assistance listing numbers with null titles')

    # Grabs list of all assistance_listing_numbers
    update_list = sess.query(PublishedFABS.assistance_listing_number).filter(
        PublishedFABS.assistance_listing_title.is_(None))

    count = update_list.count()
    total = count
    complete = 0

    update_list = update_list.group_by(PublishedFABS.assistance_listing_number)

    logger.info('%s null assistance listing titles found %s', count, [row[0] for row in update_list.all()])

    assistance_listing_count = 0
    for row in update_list:
        start_time = datetime.datetime.now()
        current_assistance_listing = row.assistance_listing_number
        assistance_listing_query = sess.query(PublishedFABS).\
            filter(PublishedFABS.assistance_listing_title.is_(None),
                   PublishedFABS.assistance_listing_number == row.assistance_listing_number)
        if len(current_assistance_listing.split('.')) < 2:
            logger.info('assistance_listing_number %s is not a valid assistance_listing_number',
                        current_assistance_listing)
            invalid_count = assistance_listing_query.count()
            count = count - invalid_count
            complete = complete + invalid_count
            logger.info('%s entries are invalid', invalid_count)
            continue
        # auto pad to get to length 6 because of how we are holding the assistance listing numbers
        while len(current_assistance_listing.split('.')[1]) < 3:
            current_assistance_listing = "%s%s" % (current_assistance_listing, '0')
        if current_assistance_listing not in assistance_listing_list:
            logger.info('assistance_listing_number %s is not a valid assistance_listing_number',
                        current_assistance_listing)
            invalid_count = assistance_listing_query.count()
            count = count - invalid_count
            complete = complete + invalid_count
            logger.info('%s entries are invalid', invalid_count)
            continue
        assistance_listing_query = assistance_listing_query.update(
            {"assistance_listing_title": assistance_listing_list[current_assistance_listing]},
            synchronize_session=False)

        count = count - assistance_listing_query
        complete = complete + assistance_listing_query
        end_time = datetime.datetime.now()
        logger.info('%s entries updated, %s entries remaining, %s percent complete',
                    complete, count, (complete / total) * 100)
        logger.info('Assistance Listing %s took %s to complete', row.assistance_listing_number, end_time - start_time)
        logger.info('%s entries with assistance_listing_number %s have been updated with title "%s"',
                    assistance_listing_query, row.assistance_listing_number,
                    assistance_listing_list[current_assistance_listing])
        assistance_listing_count += 1
    logger.info('%s PublishedFABS records updated', assistance_listing_count)
    sess.commit()


def main():
    start = datetime.datetime.now()
    sess = GlobalDB.db().session
    logger.info('Assistance Listing update started')
    update_assistance_listing(sess)
    end = datetime.datetime.now()
    logger.info('Assistance Listing update finished in %s seconds', end - start)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
