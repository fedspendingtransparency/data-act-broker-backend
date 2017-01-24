import unittest
from datetime import datetime, timedelta
import os
from random import randint

import boto.s3
from webtest import TestApp
from boto.s3.key import Key

from dataactvalidator.app import create_app
from dataactcore.interfaces.function_bag import check_number_of_errors_by_job_id
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import JOB_STATUS_DICT, FILE_STATUS_DICT
from dataactcore.scripts.databaseSetup import drop_database
from dataactcore.scripts.setupJobTrackerDB import setup_job_tracker_db
from dataactcore.scripts.setupErrorDB import setup_error_db
from dataactcore.scripts.setupValidationDB import setup_validation_db
from dataactcore.utils.report import report_file_name
from dataactcore.aws.s3UrlHandler import S3UrlHandler
from dataactcore.models.jobModels import Job, Submission
from dataactcore.models.errorModels import File
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

    def assert_file_size_appxy(self, size, *suffix):
        """Locate a file on the file system and verify that its size is within
        a range (5 bytes on either side). File sizes may vary due to line
        endings, submission id size, etc.

        :param suffix: list of path components to append to
            self.local_file_directory
        """
        path = os.path.join(self.local_file_directory, *suffix)
        self.assertTrue(os.path.exists(path), "Expecting {} to exist".format(path))
        actual_size = os.path.getsize(path)
        self.assertGreater(actual_size, size - 5)
        self.assertLess(actual_size, size + 5)

    def run_test(self, job_id, status_id, status_name, file_size, staging_rows, error_status, num_errors,
                 num_warnings=0, warning_file_size=None):
        """ Runs a validation test

        Args:
            job_id: ID of job for this validation
            status_id: Expected HTTP status code for this test
            status_name: Expected status in job tracker, False if job should not exist
            file_size: Expected file size of error report, False if error report should not exist
            staging_rows: Expected number of rows in validation db staging tables. False if no rows are expected
            error_status: Expected status in file table of error DB, False if file object should not exist
            num_errors: Expected number of errors
            num_warnings: Expected number of warnings
            warning_file_size: Expected size of warning file

        Returns:

        """
        with create_app().app_context():
            sess = GlobalDB.db().session

            response = self.validate_job(job_id)
            self.assertEqual(response.status_code, status_id, str(get_response_info(response)))

            # get the job from db
            job = sess.query(Job).filter(Job.job_id == job_id).one()
            if status_name is not False:
                self.assertEqual(job.job_status_id, JOB_STATUS_DICT[status_name])

            self.assertEqual(
                response.headers.get("Content-Type"), "application/json")

            # Check valid row count for this job
            if staging_rows is not False:
                self.assertEqual(job.number_of_rows_valid, staging_rows)

            if error_status is not False:
                self.assertEqual(
                    sess.query(File).filter(File.job_id == job_id).one().file_status_id,
                    FILE_STATUS_DICT[error_status]
                )
                self.assertEqual(check_number_of_errors_by_job_id(job_id, 'fatal'), num_errors)
                self.assertEqual(check_number_of_errors_by_job_id(job_id, 'warning'), num_warnings)

            if file_size is not False:
                report_path = report_file_name(job.submission_id, False, job.file_type.name)
                if self.local:
                    self.assert_file_size_appxy(file_size, report_path)
                else:
                    self.assertGreater(S3UrlHandler.get_file_size('errors/{}'.format(report_path)), file_size - 5)
                    self.assertLess(S3UrlHandler.get_file_size('errors/{}'.format(report_path)), file_size + 5)

            if warning_file_size is not None and warning_file_size is not False:
                report_path = report_file_name(job.submission_id, True, job.file_type.name)
                if self.local:
                    self.assert_file_size_appxy(warning_file_size, report_path)
                else:
                    self.assertGreater(S3UrlHandler.get_file_size('errors/{}'.format(report_path)),
                                       warning_file_size - 5)
                    self.assertLess(S3UrlHandler.get_file_size('errors/{}'.format(report_path)), warning_file_size + 5)

        return response

    def validate_job(self, job_id):
        """ Send request to validate specified job """
        post_json = {"job_id": job_id}
        response = self.app.post_json('/validate/', post_json, expect_errors=True)
        return response

    @classmethod
    def insert_submission(cls, sess, user_id=None, reporting_end_date=None):
        """Insert submission and return id."""
        if reporting_end_date is None:
            reporting_start_date = cls.SUBMISSION_START_DEFAULT
            reporting_end_date = cls.SUBMISSION_END_DEFAULT
        else:
            reporting_start_date = reporting_end_date - timedelta(days=30)
        sub = Submission(
            datetime_utc=datetime.utcnow(),
            user_id=user_id,
            reporting_start_date=reporting_start_date,
            reporting_end_date=reporting_end_date)
        sess.add(sub)
        sess.commit()
        return sub.submission_id

    @classmethod
    def upload_file(cls, filename, user):
        """ Upload file to S3 and return S3 filename"""
        if len(filename.strip()) == 0:
            return ""

        bucket_name = CONFIG_BROKER['aws_bucket']
        region_name = CONFIG_BROKER['aws_region']

        full_path = os.path.join(CONFIG_BROKER['path'], "tests", "integration", "data", filename)

        if cls.local:
            # Local version just stores full path in job tracker
            return full_path
        else:
            # Create file names for S3
            s3_file_name = str(user) + "/" + filename

            if cls.uploadFiles:
                # Use boto to put files on S3
                s3conn = boto.s3.connect_to_region(region_name)
                key = Key(s3conn.get_bucket(bucket_name))
                key.key = s3_file_name
                bytes_written = key.set_contents_from_filename(full_path)

                assert(bytes_written > 0)
            return s3_file_name


def get_response_info(response):
    """ Format response object in readable form """
    info = 'status_code: {}'.format(response.status_code)
    if response.content_type.endswith(('+json', '/json')):
        json = response.json
        if 'errorType' in json:
            info = '{}{}errorType: {}'.format(info, os.linesep, json['errorType'])
        if 'message' in json:
            info = '{}{}message: {}'.format(info, os.linesep, json['message'])
        if 'trace' in json:
            info = '{}{}trace: {}'.format(info, os.linesep, json['trace'])
        if 'wrappedType' in json:
            info = '{}{}wrappedType: {}'.format(info, os.linesep, json['wrappedType'])
        if 'wrappedMessage' in json:
            info = '{}{}wrappedMessage: {}'.format(info, os.linesep, json['wrappedMessage'])
    else:
        info = '{}{}{}'.format(info, os.linesep, response.body)
    return info
