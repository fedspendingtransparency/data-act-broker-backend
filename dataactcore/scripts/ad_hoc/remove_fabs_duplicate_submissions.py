import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging

from dataactcore.models.jobModels import PublishedFilesHistory, CertifyHistory, PublishHistory, Submission
from dataactcore.models.userModel import User  # noqa
from dataactcore.models.lookups import PUBLISH_STATUS_DICT

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    """ Cleans up duplicated FABS published records and unpublishes the submissions they're associated with if all
        records from a specific submission are deleted.
    """

    with create_app().app_context():
        configure_logging()

        sess = GlobalDB.db().session

        logger.info("Beginning script to clean up duplicated FABS records. Creating temporary table.")

        # Create a temporary table
        sess.execute("""CREATE TEMP TABLE duplicated_fabs AS
                            SELECT UPPER(afa_generated_unique) as afa_generated_unique, MAX(submission_id) AS max_id
                            FROM published_fabs
                            WHERE is_active IS TRUE
                            GROUP BY UPPER(afa_generated_unique)
                            HAVING COUNT(1) > 1""")

        logger.info("Table created, determining which submissions have been affected.")
        # Figure out exactly which submissions have been affected in any way
        executed = sess.execute(""" SELECT DISTINCT submission_id
                                    FROM published_fabs AS pf
                                    WHERE is_active IS TRUE
                                    AND EXISTS (SELECT 1
                                                FROM duplicated_fabs AS df
                                                WHERE df.afa_generated_unique = UPPER(pf.afa_generated_unique))""")
        affected_submissions = []
        for row in executed:
            affected_submissions.append(row['submission_id'])

        # If no rows are affected, just exit, no need to hit the DB anymore
        if len(affected_submissions) == 0:
            logger.info("There are no duplicated submissions, ending script.")
            exit(0)

        logger.info("Deleting duplicate records.")
        # Delete duplicates from the published FABS table, keeping the instance with the highest submission_id
        executed = sess.execute(""" DELETE FROM published_fabs AS pf
                                    WHERE is_active IS TRUE
                                        AND EXISTS (SELECT 1
                                            FROM duplicated_fabs AS df
                                            WHERE df.afa_generated_unique = UPPER(pf.afa_generated_unique)
                                                AND df.max_id != pf.submission_id)""")

        logger.info("Deleted {} duplicate rows from published_fabs. Determining if any "
                    "submissions have been completely invalidated by the deletes.".format(executed.rowcount))

        # Make a list of submissions that have had all published records deleted
        cleared_submissions = []
        for sub in affected_submissions:
            executed = sess.execute(""" SELECT COUNT(*) as result_count
                                        FROM published_fabs
                                        WHERE submission_id = {}""".format(sub))
            if executed.fetchone()['result_count'] == 0:
                cleared_submissions.append(sub)

        # If no submission has been cleared out completely, we can just exit
        if len(cleared_submissions) == 0:
            logger.info("No affected submissions have been completely invalidated by the deletes, ending script.")
            exit(0)

        logger.info("The following submissions have been completely invalidated by the deletes, unpublishing them: "
                    + ", ".join(str(sub) for sub in cleared_submissions))

        # Unpublish each submission that has been cleared out, including deleting any record of it in the
        # certified/published tables
        for sub in cleared_submissions:
            sess.query(PublishedFilesHistory).filter_by(submission_id=sub).delete()
            sess.query(CertifyHistory).filter_by(submission_id=sub).delete()
            sess.query(PublishHistory).filter_by(submission_id=sub).delete()
            sess.query(Submission).filter_by(submission_id=sub).\
                update({"publish_status_id": PUBLISH_STATUS_DICT["unpublished"]})
        sess.commit()
        logger.info("Submissions successfully unpublished, script completed.")
