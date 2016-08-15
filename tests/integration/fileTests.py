import unittest
import os
import inspect
import boto
from datetime import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from tests.integration.baseTestAPI import BaseTestAPI
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import Submission, Job
from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.config import CONFIG_BROKER
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from shutil import copy

class FileTests(BaseTestAPI):
    """Test file submission routes."""

    updateSubmissionId = None
    filesSubmitted = False
    submitFilesResponse = None

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
            cls.jobTracker, cls.submission_user_id, cgac_code = "SYS", startDate = "10/2015", endDate = "06/2016", is_quarter = True)

        cls.jobIdDict = cls.setupJobsForStatusCheck(cls.interfaces,
            cls.status_check_submission_id)

        # setup submission/jobs data for test_error_report
        cls.error_report_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id, cgac_code = "SYS")
        cls.setupJobsForReports(cls.jobTracker, cls.error_report_submission_id)

        # setup file status data for test_metrics
        cls.test_metrics_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id, cgac_code = "SYS")
        cls.setupFileData(cls.jobTracker, cls.errorDatabase,
            cls.test_metrics_submission_id)

        cls.row_error_submission_id = cls.insertSubmission(
            cls.jobTracker, cls.submission_user_id, cgac_code = "SYS", startDate = "10/2015", endDate = "06/2016", is_quarter = True)
        cls.setupSubmissionWithError(cls.interfaces, cls.row_error_submission_id)

    def setUp(self):
        """Test set-up."""
        super(FileTests, self).setUp()
        self.login_other_user(
            self.test_users["submission_email"], self.user_password)
        # Here to repopulate BaseInterface.interfaces after login route clears them
        InterfaceHolder()

    def call_file_submission(self):
        """Call the broker file submission route."""
        if not self.filesSubmitted:
            if(CONFIG_BROKER["use_aws"]):
                self.filenames = {"appropriations":"test1.csv",
                    "award_financial":"test2.csv",
                    "program_activity":"test4.csv", "cgac_code": "SYS",
                    "reporting_period_start_date":"01/2001",
                    "reporting_period_end_date":"01/2001", "is_quarter":True}
            else:
                # If local must use full destination path
                filePath = CONFIG_BROKER["broker_files"]
                self.filenames = {"appropriations":os.path.join(filePath,"test1.csv"),
                    "award_financial":os.path.join(filePath,"test2.csv"),
                    "program_activity":os.path.join(filePath,"test4.csv"), "cgac_code": "SYS",
                    "reporting_period_start_date":"01/2001",
                    "reporting_period_end_date":"01/2001", "is_quarter":True}
            self.submitFilesResponse = self.app.post_json("/v1/submit_files/", self.filenames, headers={"x-session-id":self.session_id})
            self.updateSubmissionId = self.submitFilesResponse.json["submission_id"]
        return self.submitFilesResponse

    def test_file_submission(self):
        """Test broker file submission and response."""
        response = self.call_file_submission()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")

        json = response.json
        self.assertIn("test1.csv", json["appropriations_key"])
        self.assertIn("test2.csv", json["award_financial_key"])
        self.assertIn(CONFIG_BROKER["d2_file_name"], json["award_key"])
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
        fileKeys = ["program_activity", "award_financial",
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
        self.assertEqual(submission.user_id, self.submission_user_id)
        # Check that new submission is unpublished
        self.assertEqual(submission.publish_status_id, self.interfaces.jobDb.getPublishStatusId("unpublished"))

        # Call upload complete route
        finalizeResponse = self.check_upload_complete(
            responseDict["appropriations_id"])
        self.assertEqual(finalizeResponse.status_code, 200)

    def test_update_submission(self):
        """ Test submit_files with an existing submission ID """
        self.call_file_submission()
        if(CONFIG_BROKER["use_aws"]):
            updateJson = {"existing_submission_id": self.updateSubmissionId,
                "award_financial":"updated.csv",
                "reporting_period_start_date":"02/2016",
                "reporting_period_end_date":"03/2016"}
        else:
            # If local must use full destination path
            filePath = CONFIG_BROKER["broker_files"]
            updateJson = {"existing_submission_id": self.updateSubmissionId,
                "award_financial": os.path.join(filePath,"updated.csv"),
                "reporting_period_start_date":"02/2016",
                "reporting_period_end_date":"03/2016"}
        # Mark submission as published
        updateSubmission = self.interfaces.jobDb.getSubmissionById(self.updateSubmissionId)
        updateSubmission.publish_status_id = self.interfaces.jobDb.getPublishStatusId("published")
        self.interfaces.jobDb.session.commit()
        updateResponse = self.app.post_json("/v1/submit_files/", updateJson, headers={"x-session-id":self.session_id})
        self.assertEqual(updateResponse.status_code, 200)
        self.assertEqual(updateResponse.headers.get("Content-Type"), "application/json")

        json = updateResponse.json
        self.assertIn("updated.csv", json["award_financial_key"])
        submissionId = json["submission_id"]
        submission = self.interfaces.jobDb.getSubmissionById(submissionId)
        self.assertEqual(submission.cgac_code,"SYS") # Should not have changed agency name
        self.assertEqual(submission.reporting_start_date.strftime("%m/%Y"),"02/2016")
        self.assertEqual(submission.reporting_end_date.strftime("%m/%Y"),"03/2016")
        self.assertEqual(submission.publish_status_id, self.interfaces.jobDb.getPublishStatusId("updated"))

    def test_bad_quarter_or_month(self):
        """ Test file submissions for Q5, 13, and AB, and year of ABCD """
        updateJson = {"existing_submission_id": self.updateSubmissionId,
            "award_financial":"updated.csv",
            "reporting_period_start_date":"12/2016",
            "reporting_period_end_date":"13/2016"}
        updateResponse = self.app.post_json("/v1/submit_files/", updateJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.assertEqual(updateResponse.status_code, 400)
        self.assertIn("Date must be provided as",updateResponse.json["message"])

        updateJson = {"existing_submission_id": self.updateSubmissionId,
            "award_financial":"updated.csv",
            "reporting_period_start_date":"AB/2016",
            "reporting_period_end_date":"CD/2016"}
        updateResponse = self.app.post_json("/v1/submit_files/", updateJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.assertEqual(updateResponse.status_code, 400)
        self.assertIn("Date must be provided as",updateResponse.json["message"])

        updateJson = {"existing_submission_id": self.updateSubmissionId,
            "award_financial":"updated.csv",
            "reporting_period_start_date":"Q1/ABCD",
            "reporting_period_end_date":"Q2/2016"}
        updateResponse = self.app.post_json("/v1/submit_files/", updateJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.assertEqual(updateResponse.status_code, 400)
        self.assertIn("Date must be provided as",updateResponse.json["message"])

    def test_check_status_no_login(self):
        """ Test response with no login """
        self.logout()
        postJson = {"submission_id": self.status_check_submission_id}
        response = self.app.post_json("/v1/check_status/", postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        # Assert 401 status
        self.assertEqual(response.status_code,401)

    def test_check_status_no_session_id(self):
        """ Test response with no session ID """
        postJson = {"submission_id": self.status_check_submission_id}
        response = self.app.post_json("/v1/check_status/", postJson, expect_errors=True)
        # Assert 401 status
        self.assertEqual(response.status_code,401)

    def test_check_status_permission(self):
        """ Test that other users do not have access to status check submission """
        postJson = {"submission_id": self.status_check_submission_id}
        # Log in as non-admin user
        self.login_approved_user()
        # Call check status route
        response = self.app.post_json("/v1/check_status/", postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code,400)

    def test_check_status_admin(self):
        """ Test that admins have access to other user's submissions """
        postJson = {"submission_id": self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        # Call check status route (also checking case insensitivity of header here)
        response = self.app.post_json("/v1/check_status/", postJson, expect_errors=True, headers={"x-SESSION-id":self.session_id})
        # Assert 200 status
        self.assertEqual(response.status_code,200)



    def test_check_status(self):
        """Test broker status route response."""
        postJson = {"submission_id": self.status_check_submission_id}
        # Populating error info before calling route to avoid changing last update time
        submission = self.interfaces.jobDb.getSubmissionById(self.status_check_submission_id)

        self.interfaces.jobDb.populateSubmissionErrorInfo(self.status_check_submission_id)

        response = self.app.post_json("/v1/check_status/", postJson, headers={"x-session-id":self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        json = response.json
        # response ids are coming back as string, so patch the jobIdDict
        jobIdDict = {k: str(self.jobIdDict[k]) for k in self.jobIdDict.keys()}
        jobList = json["jobs"]
        appropJob = None
        crossJob = None
        for job in jobList:
            if str(job["job_id"]) == str(jobIdDict["appropriations"]):
                # Found the job to be checked
                appropJob = job
            elif str(job["job_id"]) == str(jobIdDict["cross_file"]):
                # Found cross file job
                crossJob = job

        # Must have an approp job and cross-file job
        self.assertNotEqual(appropJob, None)
        self.assertNotEqual(crossJob, None)
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
        self.assertEqual(appropJob["error_type"], "row_errors")

        # Check error metadata for specified error
        ruleErrorData = None
        for data in appropJob["error_data"]:
            if data["field_name"] == "header_three":
                ruleErrorData = data
        self.assertIsNotNone(ruleErrorData)
        self.assertEqual(ruleErrorData["field_name"],"header_three")
        self.assertEqual(ruleErrorData["error_name"],"rule_failed")
        self.assertEqual(ruleErrorData["error_description"],"A rule failed for this value")
        self.assertEqual(ruleErrorData["occurrences"],"7")
        self.assertEqual(ruleErrorData["rule_failed"],"Header three value must be real")
        self.assertEqual(ruleErrorData["original_label"],"A1")
        # Check warning metadata for specified warning
        warningErrorData = None
        for data in appropJob["warning_data"]:
            if data["field_name"] == "header_three":
                warningErrorData = data
        self.assertIsNotNone(warningErrorData)
        self.assertEqual(warningErrorData["field_name"],"header_three")
        self.assertEqual(warningErrorData["error_name"],"rule_failed")
        self.assertEqual(warningErrorData["error_description"],"A rule failed for this value")
        self.assertEqual(warningErrorData["occurrences"],"7")
        self.assertEqual(warningErrorData["rule_failed"],"Header three value looks odd")
        self.assertEqual(warningErrorData["original_label"],"A2")

        ruleErrorData = None
        for data in crossJob["error_data"]:
            if data["field_name"] == "header_four":
                ruleErrorData = data

        self.assertEqual(ruleErrorData["source_file"],"appropriations")
        self.assertEqual(ruleErrorData["target_file"],"award")

        # Check submission metadata
        self.assertEqual(json["cgac_code"], "SYS")
        self.assertEqual(json["reporting_period_start_date"], "Q1/2016")
        self.assertEqual(json["reporting_period_end_date"], "Q3/2016")

        # Check submission level info
        self.assertEqual(json["number_of_errors"],17)
        self.assertEqual(json["number_of_rows"],667)
        # Check number of errors and warnings in submission table

        self.assertEqual(submission.number_of_errors, 17)
        self.assertEqual(submission.number_of_warnings, 7)

        # Check that submission was created today, this test may fail if run right at midnight UTC
        self.assertEqual(json["created_on"],datetime.utcnow().strftime("%m/%d/%Y"))
        self.assertEqual(json["last_updated"],self.interfaces.jobDb.getSubmissionById(self.status_check_submission_id).updated_at.strftime("%Y-%m-%dT%H:%M:%S"))

    def test_list_submissions(self):
        """ Check list submissions route on status check submission """
        response = self.app.get("/v1/list_submissions/", headers={"x-session-id":self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        json = response.json
        errorReportSub = None
        errorSub = None
        for submission in json["submissions"]:
            if submission["submission_id"] == self.error_report_submission_id:
                errorReportSub = submission
            elif submission["submission_id"] == self.row_error_submission_id:
                errorSub = submission
        self.assertIsNotNone(errorReportSub)
        self.assertIsNotNone(errorSub)
        self.assertEqual(errorReportSub["status"], "validation_successful")
        self.assertEqual(errorSub["status"], "validation_errors")

    def test_get_protected_files(self):
        """ Check get_protected_files route """

        if CONFIG_BROKER["use_aws"]:
            response = self.app.get("/v1/get_protected_files/", headers={"x-session-id": self.session_id})
            self.assertEqual(response.status_code, 200, msg=str(response.json))
            self.assertEqual(response.headers.get("Content-Type"), "application/json")
            json = response.json
            self.assertNotEqual(len(json["urls"]), 0)
        else:
            response = self.app.get("/v1/get_protected_files/", headers={"x-session-id": self.session_id}, expect_errors=True)
            self.assertEqual(response.status_code, 400, msg=str(response.json))
            self.assertEqual(response.headers.get("Content-Type"), "application/json")
            json = response.json
            self.assertEqual(json["urls"], {})

    def check_upload_complete(self, jobId):
        """Check status of a broker file submission."""
        postJson = {"upload_id": jobId}
        return self.app.post_json("/v1/finalize_job/", postJson, headers={"x-session-id":self.session_id})

    @staticmethod
    def uploadFileByURL(s3FileName,filename):
        """Upload file and return filename and bytes written."""
        fullPath = os.path.join(CONFIG_BROKER['path'], "tests", "integration", "data", filename)

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
            s3conn = boto.s3.connect_to_region(CONFIG_BROKER["aws_region"])
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
            "/v1/submission_error_reports/", postJson, headers={"x-session-id":self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 14)
        self.assertIn("cross_appropriations-program_activity", response.json)

    def test_warning_reports(self):
        """Test broker csv_validation error report."""
        postJson = {"submission_id": self.error_report_submission_id}
        response = self.app.post_json(
            "/v1/submission_warning_reports/", postJson, headers={"x-session-id":self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 14)
        self.assertIn("cross_warning_appropriations-program_activity", response.json)

    def check_metrics(self, submission_id, exists, type_file) :
        """Get error metrics for specified submission."""
        postJson = {"submission_id": submission_id}
        response = self.app.post_json("/v1/error_metrics/", postJson, headers={"x-session-id":self.session_id})

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
    def insertSubmission(jobTracker, submission_user_id, submission=None, cgac_code = None, startDate = None, endDate = None, is_quarter = False):
        """Insert one submission into job tracker and get submission ID back."""
        if submission:
            sub = Submission(submission_id=submission,
                datetime_utc=datetime.utcnow(), user_id=submission_user_id, cgac_code = cgac_code, reporting_start_date = JobHandler.createDate(startDate), reporting_end_date = JobHandler.createDate(endDate), is_quarter_format = is_quarter)
        else:
            sub = Submission(datetime_utc=datetime.utcnow(), user_id=submission_user_id, cgac_code = cgac_code, reporting_start_date = JobHandler.createDate(startDate), reporting_end_date = JobHandler.createDate(endDate), is_quarter_format = is_quarter)
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @staticmethod
    def insertJob(jobTracker, filetype, status, type_id, submission, job_id=None, filename = None, file_size = None, num_rows = None):
        """Insert one job into job tracker and get ID back."""
        job = Job(
            file_type_id=filetype,
            job_status_id=status,
            job_type_id=type_id,
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
    def insertFile(errorDB, job, status):
        """Insert one file into error database and get ID back."""
        fs = File(
            job_id=job,
            filename=' ',
            file_status_id=status
        )
        errorDB.session.add(fs)
        errorDB.session.commit()
        return fs.file_id

    @staticmethod
    def insertRowLevelError(errorDB, job):
        """Insert one error into error database."""
        #TODO: remove hard-coded surrogate keys and filename
        ed = ErrorMetadata(
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
        return ed.error_metadata_id

    @staticmethod
    def setupSubmissionWithError(interfaces, row_error_submission_id):
        """ Set up a submission that will come back with a status of validation_errors """
        jobValues = {}
        jobValues["awardFin"] = [3, 4, 2, "awardFin.csv", 100, 100]
        jobValues["appropriations"] = [1, 4, 2, "approp.csv", 2345, 567]
        jobValues["program_activity"] = [2, 4, 2, "programActivity.csv", None, None]
        jobValues["cross_file"] = [None,4,4,2,None,None,None]
        for jobKey, values in jobValues.items():
            job_id = FileTests.insertJob(
                interfaces.jobDb,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=row_error_submission_id,
                filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
        # Add errors to cross file job
        metadata = ErrorMetadata(job_id = job_id, occurrences = 2, severity_id = interfaces.validationDb.getRuleSeverityId("fatal"))
        interfaces.errorDb.session.add(metadata)
        interfaces.errorDb.session.commit()

    @staticmethod
    def setupJobsForStatusCheck(interfaces, submission_id):
        """Set up test jobs for job status test."""

        # TODO: remove hard-coded surrogate keys
        jobValues = {}
        jobValues["uploadFinished"] = [4, 4, 1, None, None, None]
        jobValues["recordRunning"] = [4, 3, 2, None, None, None]
        jobValues["externalWaiting"] = [4, 1, 5, None, None, None]
        jobValues["awardFin"] = [3, 2, 2, "awardFin.csv", 100, 100]
        jobValues["appropriations"] = [1, 2, 2, "approp.csv", 2345, 567]
        jobValues["program_activity"] = [2, 2, 2, "programActivity.csv", None, None]
        jobValues["cross_file"] = [None,4,4,2,None,None,None]
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

        # For appropriations job, create an entry in file for this job
        fileRec = File(job_id=jobIdDict["appropriations"],
                       filename="approp.csv",
                       file_status_id=interfaces.errorDb.getFileStatusId("complete"),
                       headers_missing="missing_header_one, missing_header_two",
                       headers_duplicated="duplicated_header_one, duplicated_header_two",
                       row_errors_present=True)
        crossFile = File(job_id=jobIdDict["cross_file"],
                       filename="approp.csv",
                       file_status_id=interfaces.errorDb.getFileStatusId("complete"),
                       headers_missing="",
                       headers_duplicated="",
                       row_errors_present=True)
        interfaces.errorDb.session.add(fileRec)
        interfaces.errorDb.session.add(crossFile)

        # Put some entries in error data for approp job
        ruleError = ErrorMetadata(job_id = jobIdDict["appropriations"], filename = "approp.csv", field_name = "header_three",
                                  error_type_id = 6, occurrences = 7, rule_failed = "Header three value must be real", original_rule_label = "A1",
                                  file_type_id = interfaces.validationDb.getFileTypeIdByName("appropriations"),
                                  target_file_type_id = interfaces.validationDb.getFileTypeIdByName("award"),
                                  severity_id = interfaces.validationDb.getRuleSeverityId("fatal"))
        warningError = ErrorMetadata(job_id = jobIdDict["appropriations"], filename = "approp.csv", field_name = "header_three",
                                  error_type_id = 6, occurrences = 7, rule_failed = "Header three value looks odd", original_rule_label = "A2",
                                  file_type_id = interfaces.validationDb.getFileTypeIdByName("appropriations"),
                                  target_file_type_id = interfaces.validationDb.getFileTypeIdByName("award"),
                                  severity_id = interfaces.validationDb.getRuleSeverityId("warning"))
        reqError = ErrorMetadata(job_id = jobIdDict["appropriations"], filename = "approp.csv", field_name = "header_four", error_type_id = 2, occurrences = 5, rule_failed = "A required value was not provided", severity_id = interfaces.validationDb.getRuleSeverityId("fatal"))
        interfaces.errorDb.session.add(ruleError)
        interfaces.errorDb.session.add(warningError)
        interfaces.errorDb.session.add(reqError)
        crossError = ErrorMetadata(job_id = jobIdDict["cross_file"], filename = "approp.csv", field_name = "header_four",
                                   error_type_id = 2, occurrences = 5, rule_failed = "A required value was not provided",
                                   file_type_id = interfaces.validationDb.getFileTypeIdByName("appropriations"),
                                   target_file_type_id = interfaces.validationDb.getFileTypeIdByName("award"),
                                   severity_id = interfaces.validationDb.getRuleSeverityId("fatal"))

        interfaces.errorDb.session.add(crossError)
        interfaces.errorDb.session.commit()

        return jobIdDict

    @staticmethod
    def setupJobsForReports(jobTracker, error_report_submission_id):
        """Setup jobs table for checking validator unit test error reports."""
        FileTests.insertJob(jobTracker, filetype=4, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=3, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=1, status=4, type_id=2,
            submission=error_report_submission_id)
        FileTests.insertJob(jobTracker, filetype=2, status=4, type_id=2,
            submission=error_report_submission_id)

    @staticmethod
    def setupFileData(jobTracker, errorDb, submission_id):
        """Setup test data for the route test"""

        # TODO: remove hard-coded surrogate keys
        job = FileTests.insertJob(
            jobTracker,
            filetype=4,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFile(errorDb, job, 1) # Everything Is Fine

        job = FileTests.insertJob(
            jobTracker,
            filetype=3,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFile(errorDb, job, 3) # Bad Header

        job = FileTests.insertJob(
            jobTracker,
            filetype=1,
            status=2,
            type_id=2,
            submission=submission_id
        )
        FileTests.insertFile(errorDb, job, 1) # Validation level Errors
        FileTests.insertRowLevelError(errorDb, job)

if __name__ == '__main__':
    unittest.main()
