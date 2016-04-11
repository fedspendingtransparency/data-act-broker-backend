import unittest
import os
import inspect
from datetime import datetime
from datetime import date
from time import sleep, time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from baseTest import BaseTest
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.errorModels import ErrorData, FileStatus
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.responseException import ResponseException
from dataactbroker.handlers.jobHandler import JobHandler
from shutil import copy

class FileTests(BaseTest):
    """Test file submission routes."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(FileTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture

        # get the submission test user
        submission_user = cls.userDb.getUserByEmail(
            cls.test_users['submission_email'])
        cls.submission_user_id = submission_user.user_id

        # setup submission/jobs data for test_check_status
        cls.status_check_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id, agency = "Department of the Treasury", startDate = "04/01/2016", endDate = "04/02/2016")

        cls.jobIdDict = cls.setupJobsForStatusCheck(cls.interfaces,
            cls.status_check_submission_id)

        # setup submission/jobs data for test_error_report
        cls.error_report_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id)
        cls.setupJobsForReports(cls.jobTracker, cls.error_report_submission_id)

        # setup file status data for test_metrics
        cls.test_metrics_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id)
        cls.setupFileStatusData(cls.jobTracker, cls.errorDatabase,
            cls.test_metrics_submission_id)

    def setUp(self):
        """Test set-up."""
        super(FileTests, self).setUp()
        self.login_other_user(
            self.test_users["submission_email"], self.user_password)

    def call_file_submission(self):
        """Call the broker file submission route."""

        if(CONFIG_BROKER["use_aws"]):
            self.filenames = {"appropriations":"test1.csv",
                "award_financial":"test2.csv", "award":"test3.csv",
                "program_activity":"test4.csv", "agency_name": "Department of the Treasury",
                "reporting_period_start_date":"01/13/2001",
                "reporting_period_end_date":"01/14/2001"}
        else:
            # If local must use full destination path
            filePath = CONFIG_BROKER["broker_files"]
            self.filenames = {"appropriations":os.path.join(filePath,"test1.csv"),
                "award_financial":os.path.join(filePath,"test2.csv"), "award":os.path.join(filePath,"test3.csv"),
                "program_activity":os.path.join(filePath,"test4.csv"), "agency_name": "Department of the Treasury",
                "reporting_period_start_date":"01/13/2001",
                "reporting_period_end_date":"01/14/2001"}

        return self.app.post_json("/v1/submit_files/", self.filenames)

    def test_file_submission(self):
        """Test broker file submission and response."""
        response = self.call_file_submission()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")

        json = response.json
        self.assertIn("test1.csv", json["appropriations_key"])
        self.assertIn("test2.csv", json["award_financial_key"])
        self.assertIn("test3.csv", json["award_key"])
        self.assertIn("test4.csv", json["program_activity_key"])
        self.assertIn("credentials", json)

        credentials = json["credentials"]
        for requiredField in ["AccessKeyId", "SecretAccessKey",
            "SessionToken", "SessionToken"]:
            self.assertIn(requiredField, credentials)
            self.assertTrue(len(credentials[requiredField]))

        self.assertIn("bucket_name", json)
        self.assertTrue(len(json["bucket_name"]))

        fileResults = self.uploadFileByURL(
            "/"+json["appropriations_key"], "test1.csv")
        self.assertGreater(fileResults['bytesWritten'], 0)

        # Test that job ids are returned
        responseDict = json
        fileKeys = ["program_activity", "award", "award_financial",
            "appropriations"]
        for key in fileKeys:
            idKey = "".join([key,"_id"])
            self.assertIn(idKey, responseDict)
            jobId = responseDict[idKey]
            self.assertIsInstance(jobId, int)
            # Check that original filenames were stored in DB
            originalFilename = self.interfaces.jobDb.getOriginalFilenameById(jobId)
            self.assertEquals(originalFilename,self.filenames[key])
        # check that submission got mapped to the correct user
        submissionId = responseDict["submission_id"]
        self.file_submission_id = submissionId
        submission = self.interfaces.jobDb.getSubmissionById(submissionId)
        self.assertEquals(submission.user_id, self.submission_user_id)



        # Call upload complete route
        finalizeResponse = self.check_upload_complete(
            responseDict["appropriations_id"])
        self.assertEqual(finalizeResponse.status_code, 200)
        # Wait for validation to complete
        start = time()
        valId = self.interfaces.jobDb.session.query(JobStatus).filter(JobStatus.submission_id == submissionId).filter(JobStatus.file_type_id == 3).filter(JobStatus.type_id == self.interfaces.jobDb.getTypeId("csv_record_validation")).one().job_id

        # First wait for job Id to get a file status
        done = False
        while not done and ((time() - start) < 20):
            try:
                self.interfaces.errorDb.checkStatusByJobId(valId)
                done = True
            except ResponseException:
                # Does not exist yet, keep trying
                sleep(1)
        while (self.interfaces.jobDb.getJobStatus(valId) == "waiting" or self.interfaces.jobDb.getJobStatus(valId) == "running") and ((time() - start) < 20):
            # If validation does not complete in 20 seconds, give up
            sleep(1)
        self.assertLess((time() - start),20,"Validation did not complete")
        self.assertEqual(self.interfaces.jobDb.getJobStatus(valId),"finished")
        self.assertEqual(self.interfaces.errorDb.checkStatusByJobId(valId),self.interfaces.errorDb.getStatusId("complete"))
        fileSize = self.interfaces.jobDb.getFileSizeById(valId)
        numRows = self.interfaces.jobDb.getNumberOfRowsById(valId)
        # Check that file size and number of rows got populated
        self.assertIsNotNone(fileSize)
        self.assertIsNotNone(numRows)

    def test_check_status(self):
        """Test broker status route response."""
        postJson = {"submission_id": self.status_check_submission_id}
        response = self.app.post_json("/v1/check_status/", postJson)

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        json = response.json
        # response ids are coming back as string, so patch the jobIdDict
        jobIdDict = {k: str(self.jobIdDict[k]) for k in self.jobIdDict.keys()}
        jobList = json["jobs"]
        appropJob = None
        for job in jobList:
            if str(job["job_id"]) == str(jobIdDict["appropriations"]):
                # Found the job to be checked
                appropJob = job
                break
        # Must have an approp job
        self.assertNotEqual(appropJob, None)
        # And that job must have the following
        self.assertEqual(appropJob["job_status"],"ready")
        self.assertEqual(appropJob["job_type"],"csv_record_validation")
        self.assertEqual(appropJob["file_type"],"appropriations")
        self.assertEqual(appropJob["filename"],"approp.csv")
        self.assertEqual(appropJob["file_status"],"complete")
        self.assertIn("missing_header_one", appropJob["missing_headers"])
        self.assertIn("missing_header_two", appropJob["missing_headers"])
        self.assertIn("duplicated_header_one", appropJob["duplicated_headers"])
        self.assertIn("duplicated_header_two", appropJob["duplicated_headers"])
        # Check file size and number of rows
        self.assertEqual(appropJob["file_size"], 2345)
        self.assertEqual(appropJob["number_of_rows"], 567)

        # Check submission metadata
        self.assertEqual(json["agency_name"], "Department of the Treasury")
        self.assertEqual(json["reporting_period_start_date"], "04/01/2016")
        self.assertEqual(json["reporting_period_end_date"], "04/02/2016")

        # Check submission level info
        self.assertEqual(json["number_of_errors"],0) # No actual validation is occurring in this test, so no errors
        self.assertEqual(json["number_of_rows"],667)
        # Check that submission was created today, this test may fail if run right at midnight UTC
        self.assertEqual(json["created_on"],datetime.utcnow().strftime("%m/%d/%Y"))

    def check_upload_complete(self, jobId):
        """Check status of a broker file submission."""
        postJson = {"upload_id": jobId}
        return self.app.post_json("/v1/finalize_job/", postJson)

    @staticmethod
    def uploadFileByURL(s3FileName,filename):
        """Upload file and return filename and bytes written."""
        path = os.path.dirname(
            os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = os.path.join(path, filename)

        if CONFIG_BROKER['local']:
            # If not using AWS, put file submission in location
            # specified by the config file
            broker_file_path = CONFIG_BROKER['broker_files']
            copy(fullPath, broker_file_path)
            submittedFile = os.path.join(broker_file_path, filename)
            return {'bytesWritten': os.path.getsize(submittedFile),
                    's3FileName': fullPath}
        else:
            # Use boto to put files on S3
            s3conn = S3Connection()
            bucketName = CONFIG_BROKER['aws_bucket']
            key = Key(s3conn.get_bucket(bucketName))
            key.key = s3FileName
            bytesWritten = key.set_contents_from_filename(fullPath)
            return {'bytesWritten': bytesWritten,
                    's3FileName': s3FileName}

    def test_error_report(self):
        """Test broker csv_validation error report."""
        postJson = {"submission_id": self.error_report_submission_id}
        response = self.app.post_json(
            "/v1/submission_error_reports/", postJson)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 4)

    def check_metrics(self, submission_id, exists, type_file) :
        """Get error metrics for specified submission."""
        postJson = {"submission_id": submission_id}
        response = self.app.post_json("/v1/error_metrics/", postJson)

        self.assertEqual(response.status_code, 200)

        type_file_length = len(response.json[type_file])
        if(exists):
            self.assertGreater(type_file_length, 0)
        else:
            self.assertEqual(type_file_length, 0)

    def test_metrics(self):
        """Test broker status record handling."""
        #Check the route
        self.check_metrics(self.test_metrics_submission_id,
            False, "award")
        self.check_metrics(self.test_metrics_submission_id,
            True, "award_financial")
        self.check_metrics(self.test_metrics_submission_id,
            True, "appropriations")

    @staticmethod
    def insertSubmission(jobTracker, submission_user_id, submission=None, agency = None, startDate = None, endDate = None):
        """Insert one submission into job tracker and get submission ID back."""
        if submission:
            sub = Submission(submission_id=submission,
                datetime_utc=datetime.utcnow(), user_id=submission_user_id, agency_name = agency, reporting_start_date = JobHandler.createDate(startDate), reporting_end_date = JobHandler.createDate(endDate))
        else:
            sub = Submission(datetime_utc=datetime.utcnow(), user_id=submission_user_id, agency_name = agency, reporting_start_date = JobHandler.createDate(startDate), reporting_end_date = JobHandler.createDate(endDate))
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @staticmethod
    def insertJob(jobTracker, filetype, status, type_id, submission, job_id=None, filename = None, file_size = None, num_rows = None):
        """Insert one job into job tracker and get ID back."""
        job = JobStatus(
            file_type_id=filetype,
            status_id=status,
            type_id=type_id,
            submission_id=submission,
            original_filename=filename,
            file_size = file_size,
            number_of_rows = num_rows
        )
        if job_id:
            job.job_id = job_id
        jobTracker.session.add(job)
        jobTracker.session.commit()
        return job.job_id

    @staticmethod
    def insertFileStatus(errorDB, job, status):
        """Insert one file status into error database and get ID back."""
        fs = FileStatus(
            job_id=job,
            filename=' ',
            status_id=status
        )
        errorDB.session.add(fs)
        errorDB.session.commit()
        return fs.file_id

    @staticmethod
    def insertRowLevelError(errorDB, job):
        """Insert one error into error database."""
        #TODO: remove hard-coded surrogate keys and filename
        ed = ErrorData(
            job_id=job,
            filename='test.csv',
            field_name='header 1',
            error_type_id=1,
            occurrences=100,
            first_row=123,
            rule_failed='Type Check'
        )
        errorDB.session.add(ed)
        errorDB.session.commit()
        return ed.error_data_id

    @staticmethod
    def setupJobsForStatusCheck(interfaces, submission_id):
        """Set up test jobs for job status test."""

        # TODO: remove hard-coded surrogate keys
        jobValues = {}
        jobValues["uploadFinished"] = [1, 4, 1, None, None, None]
        jobValues["recordRunning"] = [1, 3, 2, None, None, None]
        jobValues["externalWaiting"] = [1, 1, 5, None, None, None]
        jobValues["awardFin"] = [2, 2, 2, "awardFin.csv", 100, 100]
        jobValues["appropriations"] = [3, 2, 2, "approp.csv", 2345, 567]
        jobValues["program_activity"] = [4, 2, 2, "programActivity.csv", None, None]
        jobIdDict = {}

        for jobKey, values in jobValues.items():
            job_id = FileTests.insertJob(
                interfaces.jobDb,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=submission_id,
                filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
            jobIdDict[jobKey] = job_id

        # For appropriations job, create an entry in file_status for this job
        fileStatus = FileStatus(job_id = jobIdDict["appropriations"],filename = "approp.csv", status_id = interfaces.errorDb.getStatusId("complete"), headers_missing = "missing_header_one, missing_header_two", headers_duplicated = "duplicated_header_one, duplicated_header_two")
        interfaces.errorDb.session.add(fileStatus)
        interfaces.errorDb.session.commit()
        return jobIdDict

    @staticmethod
    def setupJobsForReports(jobTracker, error_report_submission_id):
        """Setup jobs table for checking validator unit test error reports."""
        FileTests.insertJob(jobTracker, filetype=1, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=2, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=3, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=4, status=4, type_id=2,
            submission=error_report_submission_id)

    @staticmethod
    def setupFileStatusData(jobTracker, errorDb, submission_id):
        """Setup test data for the route test"""

        # TODO: remove hard-coded surrogate keys
        job = FileTests.insertJob(
            jobTracker,
            filetype=1,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFileStatus(errorDb, job, 1) # Everything Is Fine

        job = FileTests.insertJob(
            jobTracker,
            filetype=2,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFileStatus(errorDb, job, 3) # Bad Header

        job = FileTests.insertJob(
            jobTracker,
            filetype=3,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFileStatus(errorDb, job, 1) # Validation level Errors
        FileTests.insertRowLevelError(errorDb, job)

if __name__ == '__main__':
    unittest.main()
