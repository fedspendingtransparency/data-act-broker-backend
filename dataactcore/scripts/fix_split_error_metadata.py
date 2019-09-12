import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def duplicate_removal(table):
    """ Run the sql to remove duplicates

        Args:
            table: The table to remove the duplicates from
    """
    logger.info('Fixing {} duplicated entries'.format(table))

    # Creating a temporary table storing all the duplicates
    create_table_sql = """
                CREATE TABLE IF NOT EXISTS temp_duplicated_{} (
                    job_id integer,
                    original_rule_label text,
                    occurrences integer,
                    min_id integer
                );
            """
    sess.execute(create_table_sql.format(table))
    # In case something went wrong, we don't want extra data
    sess.execute('TRUNCATE TABLE temp_duplicated_{};'.format(table))

    # Inserting a summary of all duplicated error metadata
    duplicates_insert_sql = """
                INSERT INTO temp_duplicated_{table} (job_id, original_rule_label, occurrences, min_id)
                SELECT job_id, original_rule_label, SUM(occurrences) AS occurrences, MIN({table}_id) AS min_id
                FROM {table}
                WHERE LENGTH(original_rule_label) > 0
                GROUP BY job_id, original_rule_label
                HAVING COUNT(1) > 1;
            """
    sess.execute(duplicates_insert_sql.format(table=table))

    # Updating the entry with the min ID that we're keeping
    update_sql = """
        UPDATE {table}
        SET occurrences = temp.occurrences
        FROM temp_duplicated_{table} AS temp
        WHERE temp.job_id = {table}.job_id
            AND temp.original_rule_label = {table}.original_rule_label
            AND temp.min_id = {table}.{table}_id;
    """
    sess.execute(update_sql.format(table=table))

    # Deleting extraneous data now that everything is updated
    delete_sql = """
        DELETE FROM {table}
        WHERE EXISTS (SELECT 1
            FROM temp_duplicated_{table} AS temp
            WHERE temp.job_id = {table}.job_id
                AND temp.original_rule_label = {table}.original_rule_label
                AND temp.min_id <> {table}.{table}_id)
    """
    sess.execute(delete_sql.format(table=table))

    sess.execute('DROP TABLE temp_duplicated_{}'.format(table))
    sess.commit()

    logger.info('{} duplicated entries fixed'.format(table))


if __name__ == '__main__':
    sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        duplicate_removal('error_metadata')
        duplicate_removal('certified_error_metadata')
