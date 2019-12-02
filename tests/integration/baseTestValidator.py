import unittest
from datetime import datetime, timedelta
from random import randint
from flask_bcrypt import Bcrypt
import os

from webtest import TestApp

from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import create_user_with_password
from dataactcore.scripts.database_setup import drop_database
from dataactcore.scripts.setup_job_tracker_db import setup_job_tracker_db
from dataactcore.scripts.setup_error_db import setup_error_db
from dataactcore.scripts.setup_validation_db import setup_validation_db
from dataactcore.scripts.initialize import load_sql_rules
from dataactcore.models.jobModels import Submission
from dataactcore.models.domainModels import CGAC
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER, CONFIG_DB
from dataactcore.scripts.database_setup import create_database, run_migrations
import dataactcore.config

basePath = CONFIG_BROKER["path"]
validator_config_path = os.path.join(basePath, "dataactvalidator", "config")


class BaseTestValidator(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        # TODO: refactor into a pytest class fixtures and inject as necessary
        # update application's db config options so unittests
        # run against test databases
        configure_logging()
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
        sess = GlobalDB.db().session

        # set up default e-mails for tests
        test_users = {
            'admin_user': 'data.act.tester.1@gmail.com',
            'agency_user': 'data.act.test.2@gmail.com',
            'agency_user_2': 'data.act.test.3@gmail.com',
            'no_permissions_user': 'data.act.tester.4@gmail.com',
            'editfabs_user': 'data.act.test.5@gmail.com'
        }
        admin_password = '@pprovedPassw0rdy'

        cgac = CGAC(cgac_code='000', agency_name='Example Agency')
        sess.add(cgac)
        sess.commit()

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

        # setup Schema

        SchemaLoader.load_all_from_path(validator_config_path)
        load_sql_rules()

        create_user_with_password(test_users["admin_user"], admin_password, Bcrypt(), website_admin=True)
        cls.userId = None
        cls.test_users = test_users
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
