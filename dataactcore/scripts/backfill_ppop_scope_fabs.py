import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

BACKFILL_FABS_PPOP_SCOPE_SQL_1 = """
    UPDATE published_award_financial_assistance
    SET place_of_performance_scope =
        CASE WHEN place_of_performance_code ~ '^00\*{5}$'
             THEN 'Multi-State'
             WHEN place_of_performance_code ~ '^[a-zA-Z]{2}\*{5}$'
             THEN 'State-wide'
             WHEN place_of_performance_code ~ '^[a-zA-Z]{2}\*\*\d{3}$'
             THEN 'County-wide'
             WHEN UPPER(place_of_performance_code) = '00FORGN'
             THEN 'Foreign'
         END
    WHERE (place_of_performance_zip4a IS NULL
        AND place_of_performance_scope IS NULL);
"""
BACKFILL_FABS_PPOP_SCOPE_SQL_2 = """
    UPDATE published_award_financial_assistance
    SET place_of_performance_scope =
        CASE WHEN LOWER(place_of_performance_zip4a) = 'city-wide'
             THEN 'City-wide'
             WHEN place_of_performance_zip4a ~ '^\d{5}(-?\d{4})?$'
             THEN 'Single Zip Code'
         END
    WHERE (place_of_performance_code ~ '^[a-zA-Z]{2}\d{4}[\dRr]$'
        AND place_of_performance_scope IS NULL);
"""

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        affected = 0

        logger.info('Backfilling empty place_of_performance_scope values in the fabs table (part i).')
        executed = sess.execute(BACKFILL_FABS_PPOP_SCOPE_SQL_1)
        affected += executed.rowcount
        sess.commit()

        logger.info('Backfilling empty place_of_performance_scope values in the fabs table (part ii).')
        executed = sess.execute(BACKFILL_FABS_PPOP_SCOPE_SQL_2)
        affected += executed.rowcount
        sess.commit()

        logger.info('Backfill completed, {} rows affected\n'.format(executed.rowcount))

        sess.close()
