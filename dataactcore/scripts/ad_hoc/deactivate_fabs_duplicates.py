import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    """ Deactivates all but the latest FABS records that have somehow become duplicated. """
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session

        logger.info("Beginning script to deactivate older duplicated FABS records. Creating temporary table.")

        # Create a temporary table
        sess.execute("""CREATE TEMP TABLE duplicated_fabs AS
                            SELECT UPPER(afa_generated_unique) AS afa_generated_unique,
                                MAX(published_fabs_id) AS max_id
                            FROM published_fabs
                            WHERE is_active IS TRUE
                            GROUP BY UPPER(afa_generated_unique)
                            HAVING COUNT(1) > 1""")

        logger.info("Table created, determining if duplicates exist.")
        # Figure out if there are any duplicates
        executed = sess.execute(""" SELECT COUNT(*)
                                    FROM duplicated_fabs""").fetchall()

        # If no rows are affected, just exit, no need to hit the DB anymore
        if executed[0].count == 0:
            logger.info("There are no duplicated records, ending script.")
            exit(0)

        logger.info("Deactivating duplicate records.")
        # Deactivate duplicates in the published FABS table, not touching the instance with the highest submission_id
        executed = sess.execute(""" UPDATE published_fabs AS pf
                                    SET is_active = FALSE
                                    WHERE is_active IS TRUE
                                        AND EXISTS (SELECT 1
                                            FROM duplicated_fabs AS df
                                            WHERE df.afa_generated_unique = UPPER(pf.afa_generated_unique)
                                                AND df.max_id != pf.published_fabs_id)""")

        logger.info("Deactivated {} duplicate rows in published_fabs.".format(executed.rowcount))
        sess.commit()
        logger.info("Records successfully deactivated, script completed.")
