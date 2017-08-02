from dataactcore.config import CONFIG_DB
from dataactcore.logging import configure_logging
from dataactcore.scripts.databaseSetup import create_database, run_migrations
from dataactcore.scripts.setupErrorDB import setup_error_db
from dataactcore.scripts.setupJobQueueDB import setup_job_queue_db
from dataactcore.scripts.setupJobTrackerDB import setup_job_tracker_db
from dataactcore.scripts.setupUserDB import setup_user_db
from dataactcore.scripts.setupValidationDB import setup_validation_db
from dataactcore.scripts.setupSubmissionTypeDB import setup_submission_type_db


def setup_all_db():
    """Sets up all databases"""
    create_database(CONFIG_DB['db_name'])
    run_migrations()
    setup_job_tracker_db()
    setup_error_db()
    setup_user_db()
    setup_job_queue_db()
    setup_validation_db()
    setup_submission_type_db()


if __name__ == '__main__':
    configure_logging()
    setup_all_db()
