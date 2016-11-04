import unittest
from datetime import datetime, timedelta
import os
from random import randint

import boto.s3
from webtest import TestApp
from boto.s3.key import Key

from dataactvalidator.app import createApp
from dataactcore.interfaces.function_bag import checkNumberOfErrorsByJobId
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import JOB_STATUS_DICT, FILE_STATUS_DICT
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.utils.report import get_report_path
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.jobModels import Job, Submission
from dataactcore.models.errorModels import File
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER, CONFIG_DB
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
import dataactcore.config


class BaseTestValidator(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        #TODO: refactor into a pytest class fixtures and inject as necessary
        # update application's db config options so unittests
        # run against test databases
        suite = cls.__name__.lower()
        config = dataactcore.config.CONFIG_DB
        cls.num = randint(1, 9999)
        config['db_name'] = 'unittest{}_{}_data_broker'.format(
            cls.num, suite)
        dataactcore.config.CONFIG_DB = config
        createDatabase(CONFIG_DB['db_name'])
        runMigrations()

        app = createApp()
        app.config['TESTING'] = True
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
        setupJobTrackerDB()
        # drop and re-create test error db/tables
        setupErrorDB()
        # drop and re-create test validation db
        setupValidationDB()

        cls.userId = None
        # constants to use for default submission start and end dates
        cls.SUBMISSION_START_DEFAULT = datetime(2015, 10, 1)
        cls.SUBMISSION_END_DEFAULT = datetime(2015, 10, 31)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        dropDatabase(CONFIG_DB['db_name'])

    def tearDown(self):
        """Tear down broker unit tests."""

    def assertFileSizeAppxy(self, size, *suffix):
        """Locate a file on the file system and verify that its size is within
        a range (5 bytes on either side). File sizes may vary due to line
        endings, submission id size, etc.

        :param suffix: list of path components to append to
            self.local_file_directory
        """
        path = os.path.join(self.local_file_directory, *suffix)
        self.assertTrue(os.path.exists(path),
                        "Expecting {} to exist".format(path))
        actualSize = os.path.getsize(path)
        self.assertGreater(actualSize, size - 5)
        self.assertLess(actualSize, size + 5)

    def run_test(self, jobId, statusId, statusName, fileSize, stagingRows,
                 errorStatus, numErrors, numWarnings = 0, warningFileSize = None):
        """ Runs a validation test

        Args:
            jobId: ID of job for this validation
            statusId: Expected HTTP status code for this test
            statusName: Expected status in job tracker, False if job should not exist
            fileSize: Expected file size of error report, False if error report should not exist
            stagingRows: Expected number of rows in validation db staging tables. False if no rows are expected
            errorStatus: Expected status in file table of error DB, False if file object should not exist
            numErrors: Expected number of errors
            rowErrorsPresent: Checks flag for whether row errors occurred, None to skip the check

        Returns:

        """
        with createApp().app_context():
            sess = GlobalDB.db().session

            response = self.validateJob(jobId)
            self.assertEqual(response.status_code, statusId, str(self.getResponseInfo(response)))

            # get the job from db
            job = sess.query(Job).filter(Job.job_id == jobId).one()
            if statusName is not False:
                self.assertEqual(job.job_status_id, JOB_STATUS_DICT[statusName])

            self.assertEqual(
                response.headers.get("Content-Type"), "application/json")

            # Check valid row count for this job
            if stagingRows is not False:
                self.assertEqual(job.number_of_rows_valid, stagingRows)

            if errorStatus is not False:
                self.assertEqual(
                    sess.query(File).filter(File.job_id == jobId).one().file_status_id,
                    FILE_STATUS_DICT[errorStatus]
                )
                self.assertEqual(checkNumberOfErrorsByJobId(jobId, 'fatal'), numErrors)
                self.assertEqual(checkNumberOfErrorsByJobId(jobId, 'warning'), numWarnings)

            if fileSize is not False:
                reportPath = get_report_path(job, 'error')
                if self.local:
                    self.assertFileSizeAppxy(fileSize, reportPath)
                else:
                    self.assertGreater(s3UrlHandler.getFileSize(
                        'errors/{}'.format(reportPath)), fileSize - 5)
                    self.assertLess(s3UrlHandler.getFileSize(
                        'errors/{}'.format(reportPath)), fileSize + 5)

            if warningFileSize is not None and warningFileSize is not False:
                reportPath = get_report_path(job, 'warning')
                if self.local:
                    self.assertFileSizeAppxy(warningFileSize, reportPath)
                else:
                    self.assertGreater(s3UrlHandler.getFileSize(
                        'errors/{}'.format(reportPath)), warningFileSize - 5)
                    self.assertLess(s3UrlHandler.getFileSize(
                        'errors/{}'.format(reportPath)), warningFileSize + 5)

        return response

    def validateJob(self, jobId):
        """ Send request to validate specified job """
        postJson = {"job_id": jobId}
        response = self.app.post_json('/validate/', postJson, expect_errors=True)
        return response

    @classmethod
    def insertSubmission(cls, sess, userId=None, reporting_end_date=None):
        """Insert submission and return id."""
        if reporting_end_date is None:
            reporting_start_date = cls.SUBMISSION_START_DEFAULT
            reporting_end_date = cls.SUBMISSION_END_DEFAULT
        else:
            reporting_start_date = reporting_end_date - timedelta(days=30)
        sub = Submission(
            datetime_utc=datetime.utcnow(),
            user_id=userId,
            reporting_start_date=reporting_start_date,
            reporting_end_date=reporting_end_date)
        sess.add(sub)
        sess.commit()
        return sub.submission_id

    @classmethod
    def uploadFile(cls, filename, user):
        """ Upload file to S3 and return S3 filename"""
        if len(filename.strip()) == 0:
            return ""

        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        fullPath = os.path.join(CONFIG_BROKER['path'], "tests", "integration", "data", filename)

        if cls.local:
            # Local version just stores full path in job tracker
            return fullPath
        else:
            # Create file names for S3
            s3FileName = str(user) + "/" + filename

            if(cls.uploadFiles) :
                # Use boto to put files on S3
                s3conn = boto.s3.connect_to_region(regionName)
                key = Key(s3conn.get_bucket(bucketName))
                key.key = s3FileName
                bytesWritten = key.set_contents_from_filename(fullPath)

                assert(bytesWritten > 0)
            return s3FileName

    def getResponseInfo(self, response):
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
