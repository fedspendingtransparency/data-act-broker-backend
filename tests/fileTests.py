import unittest
import os
import inspect
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from baseTest import BaseTest
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.errorModels import ErrorData, FileStatus

class FileTests(BaseTest):
    """Test file submission routes."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources like submissions and jobs."""
        super(FileTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture
        submission_user = cls.userDb.getUserByEmail(
            cls.test_users['submission_email'])
        cls.submission_user_id = submission_user.user_id

        # Create submission ID
        sub = Submission(datetime_utc=0, user_id=cls.submission_user_id)
        cls.jobTracker.session.add(sub)
        cls.jobTracker.session.commit()
        cls.submissionId = sub.submission_id

        # Create jobs
        # TODO: remove hard-coded surrogate keys
        jobValues = {}
        jobValues["uploadFinished"] = [1, 4, 1]
        jobValues["recordRunning"] = [1, 3, 2]
        jobValues["externalWaiting"] = [1, 1, 5]
        jobValues["awardFin"] = [2, 2, 2]
        jobValues["appropriations"] = [3, 2, 2]
        jobValues["procurement"] = [4, 2, 2]
        cls.jobIdDict = {}

        for jobKey, values in jobValues.items():
            job_id = cls.insertJob(
                cls.jobTracker,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=cls.submissionId
            )
            cls.jobIdDict[jobKey] = job_id

    def call_file_submission(self):
        """Call the broker file submission route."""
        fileJson = {"appropriations":"test1.csv", "award_financial":"test2.csv", "award":"test3.csv", "procurement":"test4.csv"}
        self.login_approved_user()
        return self.app.post_json("/v1/submit_files/", fileJson)

    def test_file_submission(self):
        """Test broker file submission and response."""
        response = self.call_file_submission()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")

        json = response.json
        self.assertIn("_test1.csv", json["appropriations_key"])
        self.assertIn("_test2.csv", json["award_financial_key"])
        self.assertIn("_test3.csv", json["award_key"])
        self.assertIn("_test4.csv", json["procurement_key"])
        self.assertIn("credentials", json)

        credentials = json["credentials"]
        for requiredField in ["AccessKeyId", "SecretAccessKey", "SessionToken", "SessionToken"]:
            self.assertIn(requiredField, credentials)
            self.assertTrue(len(credentials[requiredField]))

        self.assertIn("bucket_name", json)
        self.assertTrue(len(json["bucket_name"]))

        #TODO: mock out the S3 upload - should be in integration tests?
        s3Results = self.uploadFileByURL("/"+json["appropriations_key"], "test1.csv")
        self.assertGreater(s3Results['bytesWritten'], 0)

        # Test that job ids are returned
        responseDict = json
        idKeys = ["procurement_id", "award_id", "award_financial_id", "appropriations_id"]
        for key in idKeys:
            self.assertIn(key, responseDict)
            self.assertIsInstance(responseDict[key], int)

        # check that submission got mapped to the correct user
        submissionId = responseDict["submission_id"]
        submission = self.interfaces.jobDb.getSubmissionById(submissionId)
        self.assertEquals(submission.user_id, self.submission_user_id)

        # Call upload complete route for each id
        finalizeResponse = self.check_upload_complete(responseDict["appropriations_id"])
        self.assertEqual(finalizeResponse.status_code, 200)

    def test_check_status(self):
        """Test broker status route response."""
        self.login_approved_user()
        postJson = {"submission_id": self.submissionId}
        response = self.app.post_json("/v1/check_status/", postJson)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        json = response.json

        # response ids are coming back as string, so patch the jobIdDict
        jobIdDict = {k: str(self.jobIdDict[k]) for k in self.jobIdDict.keys()}
        self.assertEqual(json[jobIdDict["uploadFinished"]]["status"],"finished")
        self.assertEqual(json[jobIdDict["uploadFinished"]]["job_type"],"file_upload")
        self.assertEqual(json[jobIdDict["uploadFinished"]]["file_type"],"award")
        self.assertEqual(json[jobIdDict["recordRunning"]]["status"],"running")
        self.assertEqual(json[jobIdDict["recordRunning"]]["job_type"],"csv_record_validation")
        self.assertEqual(json[jobIdDict["recordRunning"]]["file_type"],"award")
        self.assertEqual(json[jobIdDict["externalWaiting"]]["status"],"waiting")
        self.assertEqual(json[jobIdDict["externalWaiting"]]["job_type"],"external_validation")
        self.assertEqual(json[jobIdDict["externalWaiting"]]["file_type"],"award")
        self.assertEqual(json[jobIdDict["appropriations"]]["status"],"ready")
        self.assertEqual(json[jobIdDict["appropriations"]]["job_type"],"csv_record_validation")
        self.assertEqual(json[jobIdDict["appropriations"]]["file_type"],"appropriations")

    def check_upload_complete(self, jobId):
        """Check status of a broker file submission."""
        postJson = {"upload_id": jobId}
        self.login_approved_user()
        return self.app.post_json("/v1/finalize_job/", postJson)

    @staticmethod
    def uploadFileByURL(s3FileName,filename):
        """Upload file to S3 and return S3 filename."""
        # Get bucket name
        bucketName = s3UrlHandler.getValueFromConfig("bucket")

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        # Use boto to put files on S3
        s3conn = S3Connection()
        key = Key(s3conn.get_bucket(bucketName))
        key.key = s3FileName
        bytesWritten = key.set_contents_from_filename(fullPath)
        return {'bytesWritten': bytesWritten, 's3FileName': s3FileName}

    def test_error_report(self):
        """Test broker csv_validation error report."""
        self.login_approved_user()
        #create new submission to use for error reports
        error_report_submission_id = self.insertSubmission(
            self.jobTracker, self.submission_user_id)
        self.setupJobsForReports(error_report_submission_id)
        postJson = {"submission_id": error_report_submission_id}
        response = self.app.post_json(
            "/v1/submission_error_reports/", postJson)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 4)
        self.teardownJobsForReports(error_report_submission_id)

    def check_metrics(self, submissionId, exists,type_file) :
        """Get error metrics for specified submission."""
        self.login_approved_user()
        postJson = {"submission_id": submissionId}
        response = self.app.post_json("/v1/error_metrics/", postJson)

        self.assertEqual(response.status_code, 200)

        type_file_length = len(response.json[type_file])
        if(exists):
            self.assertGreater(type_file_length, 0)
        else:
            self.assertEqual(type_file_length, 0)

    def test_metrics(self):
        """Test broker status record handling."""
        #setup the database for the route test
        submissionId = self.insertSubmission(
            self.jobTracker, self.submission_user_id)

        # TODO: remove hard-coded surrogate keys (put in pytest fixture?)
        job = self.insertJob(
            self.jobTracker,
            filetype=1,
            status=2,
            type_id=2,
            submission=submissionId
        )
        self.insertFileStatus(self.errorDatabase, job, 1) # Everything Is Fine

        job = self.insertJob(
            self.jobTracker,
            filetype=2,
            status=2,
            type_id=2,
            submission=submissionId
        )
        self.insertFileStatus(self.errorDatabase, job, 3) #Bad Header

        job = self.insertJob(
            self.jobTracker,
            filetype=3,
            status=2,
            type_id=2,
            submission=submissionId
        )
        self.insertFileStatus(self.errorDatabase, job, 1) # Validation level Errors
        self.insertRowLevelError(self.errorDatabase, job)

        #Check the route
        self.check_metrics(submissionId, False, "award")
        self.check_metrics(submissionId, True, "award_financial")
        self.check_metrics(submissionId, True, "appropriations")

    def insertSubmission(self, jobTracker, submission_user_id, submission=None):
        """Insert one submission into job tracker and get submission ID back."""
        if submission:
            sub = Submission(submission_id=submission,
                datetime_utc=0, user_id=submission_user_id)
        else:
            sub = Submission(datetime_utc=0, user_id=submission_user_id)
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @staticmethod
    def insertJob(jobTracker, filetype, status, type_id, submission, job_id=None):
        """Insert one job into job tracker and get ID back."""
        job = JobStatus(
            file_type_id=filetype,
            status_id=status,
            type_id=type_id,
            submission_id=submission
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

    def setupJobsForReports(self, error_report_submission_id):
        """Setup jobs table for checking validator unit test error reports."""
        jobTracker = self.jobTracker
        self.insertJob(jobTracker, filetype=1, status=4, type_id=2,
            submission=error_report_submission_id)
        self.insertJob(jobTracker, filetype=2, status=4, type_id=2,
            submission=error_report_submission_id)
        self.insertJob(jobTracker, filetype=3, status=4, type_id=2,
            submission=error_report_submission_id)
        self.insertJob(jobTracker, filetype=4, status=4, type_id=2,
            submission=error_report_submission_id)
        return error_report_submission_id

    def teardownJobsForReports(self, error_report_submission_id):
        """Delete jobs and submissions for validator unit test errors."""
        #TODO: add delete methods to the handlers? rollback?
        return

if __name__ == '__main__':
    unittest.main()
