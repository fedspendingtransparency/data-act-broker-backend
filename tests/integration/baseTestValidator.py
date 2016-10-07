import unittest
from datetime import datetime
import os
import inspect
import time
import boto.s3
from random import randint
from webtest import TestApp
from dataactvalidator.app import createApp
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import Job, Submission
from dataactcore.models.validationModels import FileColumn
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER, CONFIG_DB
from dataactcore.scripts.databaseSetup import createDatabase,runMigrations
import dataactcore.config

class BaseTestValidator(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        #TODO: refactor into a pytest class fixtures and inject as necessary
        # Prevent interface being reused from last suite
        BaseInterface.interfaces = None
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
        # Flag for each route call to launch a new thread
        cls.useThreads = False
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

        cls.interfaces = InterfaceHolder()
        cls.jobTracker = cls.interfaces.jobDb
        cls.stagingDb = cls.interfaces.stagingDb
        cls.errorInterface = cls.interfaces.errorDb
        cls.validationDb = cls.interfaces.validationDb
        cls.userId = 1

    def setUp(self):
        """Set up broker unit tests."""
        self.interfaces = InterfaceHolder()

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        cls.interfaces.close()
        dropDatabase(cls.interfaces.jobDb.dbName)

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
        response = self.validateJob(jobId, self.useThreads)
        jobTracker = self.jobTracker
        stagingDb = self.stagingDb
        self.assertEqual(response.status_code, statusId,
            msg="{}".format(self.getResponseInfo(response)))
        if statusName != False:
            self.waitOnJob(jobTracker, jobId, statusName, self.useThreads)
            self.assertEqual(jobTracker.getJobStatus(jobId), jobTracker.getJobStatusId(statusName))

        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")

        # Check valid row count for this job
        if stagingRows:
            numValidRows = jobTracker.getNumberOfValidRowsById(jobId)
            self.assertEqual(numValidRows, stagingRows)

        errorInterface = self.errorInterface
        if errorStatus is not False:
            self.assertEqual(errorInterface.checkFileStatusByJobId(jobId), errorInterface.getFileStatusId(errorStatus))
            self.assertEqual(errorInterface.checkNumberOfErrorsByJobId(jobId, self.validationDb,"fatal"), numErrors)
            self.assertEqual(errorInterface.checkNumberOfErrorsByJobId(jobId, self.validationDb,"warning"), numWarnings)
        if(fileSize != False):
            if self.local:
                self.assertFileSizeAppxy(
                    fileSize, jobTracker.getReportPath(jobId))
            else:
                self.assertGreater(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getReportPath(jobId)), fileSize - 5)
                self.assertLess(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getReportPath(jobId)), fileSize + 5)
        if(warningFileSize is not None and warningFileSize != False):
            if self.local:
                self.assertFileSizeAppxy(
                    warningFileSize, jobTracker.getWarningReportPath(jobId))
            else:
                self.assertGreater(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getWarningReportPath(jobId)), warningFileSize - 5)
                self.assertLess(s3UrlHandler.getFileSize(
                    "errors/"+jobTracker.getWarningReportPath(jobId)), warningFileSize + 5)

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
        job = Job(job_status_id=status, job_type_id=jobType,
            submission_id=submissionId, filename=s3Filename, file_type_id=fileType)
        session.add(job)
        session.commit()
        return job

    def waitOnJob(self, jobTracker, jobId, status, useThreads):
        """Wait until job gets set to the correct status in job tracker, this is done to wait for validation to complete when running tests."""
        currentID = jobTracker.getJobStatusId("running")
        targetStatus = jobTracker.getJobStatusId(status)
        if useThreads:
            while jobTracker.getJobStatus(jobId) == currentID:
                time.sleep(1)
            self.assertEqual(targetStatus, jobTracker.getJobStatus(jobId))
        else:
            self.assertEqual(targetStatus, jobTracker.getJobStatus(jobId))
            return

    @staticmethod
    def insertSubmission(jobTracker, userId, endDate = None):
        """Insert submission into job tracker and return submission ID"""
        if endDate is None:
            sub = Submission(datetime_utc=datetime.utcnow(), user_id=userId, reporting_start_date = datetime(2015,10,1), reporting_end_date = datetime(2015,10,31))
        else:
            sub = Submission(datetime_utc=datetime.utcnow(), user_id=userId, reporting_start_date = datetime(2015,10,1), reporting_end_date = endDate)
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

    @staticmethod
    def addFileColumn(fileId, fieldTypeId, columnName,
            description, required, session):
        """ Add information for one field

        Args:
            fileId: Which file this field is part of
            fieldTypeId: Data type found in this field
            columnName: Name of field
            description: Description of field
            required: True if field is required
            session: session object to be used for queries

        Returns:

        """
        column = FileColumn(file_id=fileId, field_types_id=fieldTypeId,
            name=columnName, description=description, required=required)
        session.add(column)
        session.commit()
        return column

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
