import unittest
from datetime import datetime
import os
import inspect
import logging
import time
import boto.s3
from random import randint
from webtest import TestApp
from dataactvalidator.app import createApp
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactvalidator.scripts.setupStagingDB import setupStagingDB
from dataactvalidator.scripts.setupValidationDB import setupValidationDB
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.jobModels import JobStatus, Submission
from dataactvalidator.models.validationModels import FileColumn
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER
import dataactcore.config

class BaseTest(unittest.TestCase):
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
        config['error_db_name'] = 'unittest{}_{}_error_data'.format(
            cls.num, suite)
        config['job_db_name'] = 'unittest{}_{}_job_tracker'.format(
            cls.num, suite)
        config['user_db_name'] = 'unittest{}_{}_user_manager'.format(
            cls.num, suite)
        config['validator_db_name'] = 'unittest{}_{}_validator'.format(
            cls.num, suite)
        config['staging_db_name'] = 'unittest{}_{}_staging'.format(
            cls.num, suite)
        dataactcore.config.CONFIG_DB = config

        app = createApp()
        app.config['TESTING'] = True
        cls.app = TestApp(app)

        # Allow us to augment default test failure msg w/ more detail
        cls.longMessage = True
        # Flag for each route call to launch a new thread
        cls.useThreads = False
        # Upload files to S3 (False = skip re-uploading on subsequent runs)
        cls.uploadFiles = True
        # Run tests for local broker or not
        cls.local = CONFIG_BROKER['local']
        # This needs to be set to the local directory for error reports if local is True
        cls.local_file_directory = CONFIG_SERVICES['error_report_path']

        # suppress INFO-level logging from Alembic migrations
        logging.disable(logging.WARN)
        # drop and re-create test job db/tables
        setupJobTrackerDB()
        # drop and re-create test error db/tables
        setupErrorDB()
        # drop and re-create test staging db
        setupStagingDB()
        # drop and re-create test vaidation db
        setupValidationDB(True)
        # reset logging defaults
        logging.disable(logging.NOTSET)

        cls.interfaces = InterfaceHolder()
        cls.jobTracker = cls.interfaces.jobDb
        cls.stagingDb = cls.interfaces.stagingDb
        cls.errorInterface = cls.interfaces.errorDb
        cls.validationDb = cls.interfaces.validationDb
        cls.userId = 1

    def setUp(self):
        """Set up broker unit tests."""

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        cls.interfaces.close()
        dropDatabase(cls.interfaces.jobDb.dbName)
        dropDatabase(cls.interfaces.errorDb.dbName)
        dropDatabase(cls.interfaces.stagingDb.dbName)
        dropDatabase(cls.interfaces.validationDb.dbName)

    def tearDown(self):
        """Tear down broker unit tests."""

    def run_test(self, jobId, statusId, statusName, fileSize, stagingRows,
                 errorStatus, numErrors, rowErrorsPresent = None):
        response = self.validateJob(jobId, self.useThreads)
        jobTracker = self.jobTracker
        stagingDb = self.stagingDb
        self.assertEqual(response.status_code, statusId,
            msg="{}".format(self.getResponseInfo(response)))
        if statusName != False:
            self.waitOnJob(jobTracker, jobId, statusName, self.useThreads)
            self.assertEqual(jobTracker.getStatus(jobId), jobTracker.getStatusId(statusName))

        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")

        tableName = response.json["table"]
        if type(stagingRows) == type(False) and not stagingRows:
            self.assertFalse(stagingDb.tableExists(tableName))
        else:
            self.assertTrue(stagingDb.tableExists(tableName))
            self.assertEqual(stagingDb.countRows(tableName), stagingRows)

        errorInterface = self.errorInterface
        if errorStatus is not False:
            self.assertEqual(errorInterface.checkStatusByJobId(jobId), errorInterface.getStatusId(errorStatus))
            self.assertEqual(errorInterface.checkNumberOfErrorsByJobId(jobId), numErrors)

        if(fileSize != False):
            if self.local:
                path = "".join(
                    [self.local_file_directory,jobTracker.getReportPath(jobId)])
                self.assertGreater(os.path.getsize(path), fileSize - 5)
                self.assertLess(os.path.getsize(path), fileSize + 5)
            else:
                self.assertGreater(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getReportPath(jobId)), fileSize - 5)
                self.assertLess(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getReportPath(jobId)), fileSize + 5)

        # Check if errors_present is set correctly
        if rowErrorsPresent is not None:
            # If no value provided, skip this check
            self.assertEqual(self.interfaces.errorDb.getRowErrorsPresent(jobId), rowErrorsPresent)
        return response

    def validateJob(self, jobId, useThreads):
        """ Send request to validate specified job """
        if useThreads:
            route = "/validate_threaded/"
        else:
            route = "/validate/"
        postJson = {"job_id": jobId}
        response = self.app.post_json(route, postJson, expect_errors=True)
        return response

    @staticmethod
    def addJob(status, jobType, submissionId, s3Filename, fileType, session):
        """ Create a job model and add it to the session """
        job = JobStatus(status_id=status, type_id=jobType,
            submission_id=submissionId, filename=s3Filename, file_type_id=fileType)
        session.add(job)
        session.commit()
        return job

    def waitOnJob(self, jobTracker, jobId, status, useThreads):
        """Wait until job gets set to the correct status in job tracker, this is done to wait for validation to complete when running tests."""
        currentID = jobTracker.getStatusId("running")
        targetStatus = jobTracker.getStatusId(status)
        if useThreads:
            while jobTracker.getStatus(jobId) == currentID:
                time.sleep(1)
            self.assertEqual(targetStatus, jobTracker.getStatus(jobId))
        else:
            self.assertEqual(targetStatus, jobTracker.getStatus(jobId))
            return

    @staticmethod
    def insertSubmission(jobTracker, userId):
        """Insert submission into job tracker and return submission ID"""
        sub = Submission(datetime_utc=datetime.utcnow(), user_id=userId)
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @classmethod
    def uploadFile(cls, filename, user):
        """ Upload file to S3 and return S3 filename"""
        if len(filename.strip()) == 0:
            return ""

        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        if cls.local:
            # Local version just stores full path in job tracker
            return fullPath
        else:
            # Create file names for S3
            s3FileName = str(user) + "/" + filename

            if(cls.uploadFiles) :
                # Use boto to put files on S3
                s3conn = S3Connection()
                s3conn = boto.s3.connect_to_region(regionName)
                key = Key(s3conn.get_bucket(bucketName))
                key.key = s3FileName
                bytesWritten = key.set_contents_from_filename(fullPath)

                assert(bytesWritten > 0)
            return s3FileName

    @staticmethod
    def addFileColumn(fileId, fieldTypeId, columnName,
            description, required, session):
        column = FileColumn(file_id=fileId, field_types_id=fieldTypeId,
            name=columnName, description=description, required=required)
        session.add(column)
        session.commit()
        return column

    def getResponseInfo(self, response):
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