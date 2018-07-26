import unittest
from datetime import datetime, timedelta
from random import randint

from webtest import TestApp

from dataactvalidator.health_check import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.scripts.databaseSetup import drop_database
from dataactcore.scripts.setupJobTrackerDB import setup_job_tracker_db
from dataactcore.scripts.setupErrorDB import setup_error_db
from dataactcore.scripts.setupValidationDB import setup_validation_db
from dataactcore.models.jobModels import Submission
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER, CONFIG_DB
from dataactcore.scripts.databaseSetup import create_database, run_migrations
import dataactcore.config


class BaseTestValidator(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        # TODO: refactor into a pytest class fixtures and inject as necessary
        # update application's db config options so unittests
        # run against test databases
        suite = cls.__name__.lower()
        config = dataactcore.config.CONFIG_DB
        cls.num = randint(1, 9999)
        config['db_name'] = 'unittest{}_{}_data_broker'.format(cls.num, suite)
        dataactcore.config.CONFIG_DB = config
        create_database(CONFIG_DB['db_name'])
        run_migrations()

        app = create_app()
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        cls.app = TestApp(app)

        # Allow us to augment default test failure msg w/ more detail
        cls.longMessage = True
        # Upload files to S3 (False = skip re-uploading on subsequent runs)
        cls.uploadFiles = True
        # Run tests for local broker or not
        cls.local = CONFIG_BROKER['local']
        # This needs to be set to the local directory for error reports if local is True
        cls.local_file_directory = CONFIG_SERVICES['error_report_path']

        # drop and re-create test job db/tables
        setup_job_tracker_db()
        # drop and re-create test error db/tables
        setup_error_db()
        # drop and re-create test validation db
        setup_validation_db()

        cls.userId = None
        # constants to use for default submission start and end dates
        cls.SUBMISSION_START_DEFAULT = datetime(2015, 10, 1)
        cls.SUBMISSION_END_DEFAULT = datetime(2015, 10, 31)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        GlobalDB.close()
        drop_database(CONFIG_DB['db_name'])

    def tearDown(self):
        """Tear down broker unit tests."""

    @classmethod
    def insert_submission(cls, sess, user_id=None, reporting_end_date=None):
        """Insert submission and return id."""
        if reporting_end_date is None:
            reporting_start_date = cls.SUBMISSION_START_DEFAULT
            reporting_end_date = cls.SUBMISSION_END_DEFAULT
        else:
            reporting_start_date = reporting_end_date - timedelta(days=30)
        sub = Submission(
            created_at=datetime.utcnow(),
            user_id=user_id,
            reporting_start_date=reporting_start_date,
            reporting_end_date=reporting_end_date)
        sess.add(sub)
        sess.commit()
        return sub.submission_id
