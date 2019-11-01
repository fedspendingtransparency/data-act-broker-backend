import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

BACKFILL_DISPLAYTAS_SF133_SQL = """
    UPDATE sf_133
    SET display_tas = 
        CONCAT(
            COALESCE(allocation_transfer_agency || '-', ''),
            COALESCE(agency_identifier || '-', ''),
            CASE WHEN availability_type_code IS NOT NULL THEN availability_type_code 
                WHEN beginning_period_of_availa IS NOT NULL AND ending_period_of_availabil IS NOT NULL THEN beginning_period_of_availa || '/' || ending_period_of_availabil
                ELSE ''
                END,
            COALESCE('-' || main_account_code, ''),
            COALESCE('-' || sub_account_code, '')
        )
    WHERE display_tas IS NULL;
"""

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        logger.info('Backfilling empty display_tas values in the sf_133 table.')
        executed = sess.execute(BACKFILL_DISPLAYTAS_SF133_SQL)
        sess.commit()

        logger.info('Backfill completed, {} rows affected\n'.format(executed.rowcount))

        sess.close()
