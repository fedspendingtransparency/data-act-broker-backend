import os
from datetime import datetime
from shutil import copy

import boto
from boto.s3.key import Key

from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.integration.baseTestAPI import BaseTestAPI
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import populate_submission_error_info
from dataactcore.models.jobModels import Submission, Job, JobDependency
from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, ERROR_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT)
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.health_check import create_app
from sqlalchemy import or_


class FileTests(BaseTestAPI):
    """Test file submission routes."""

    updateSubmissionId = None
    filesSubmitted = False
    submitFilesResponse = None

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(FileTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess
            submission_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.submission_user_id = submission_user.user_id

            other_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.other_user_id = other_user.user_id

            # setup submission/jobs data for test_check_status
            cls.status_check_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                   start_date="10/2015", end_date="12/2015",
                                                                   is_quarter=True)

            cls.generation_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                 start_date="07/2015", end_date="09/2015",
                                                                 is_quarter=True)

            cls.setup_file_generation_submission(sess)

            cls.jobIdDict = cls.setup_jobs_for_status_check(sess, cls.status_check_submission_id)

            # setup submission/jobs data for test_error_report
            cls.error_report_submission_id = cls.insert_submission(
                sess, cls.submission_user_id, cgac_code="SYS", start_date="10/2015", end_date="10/2015")
            cls.setup_jobs_for_reports(sess, cls.error_report_submission_id)

            # setup file status data for test_metrics
            cls.test_metrics_submission_id = cls.insert_submission(
                sess, cls.submission_user_id, cgac_code="SYS", start_date="08/2015", end_date="08/2015")
            cls.setup_file_data(sess, cls.test_metrics_submission_id)

            cls.row_error_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                start_date="10/2015", end_date="12/2015",
                                                                is_quarter=True, number_of_errors=1)
            cls.setup_submission_with_error(sess, cls.row_error_submission_id)

            cls.test_delete_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                  start_date="07/2015", end_date="09/2015",
                                                                  is_quarter=True)
            cls.setup_file_generation_submission(sess, submission_id=cls.test_delete_submission_id)

            cls.test_certified_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                     start_date="07/2015", end_date="09/2015",
                                                                     is_quarter=True, number_of_errors=0,
                                                                     publish_status_id=2)

            cls.test_uncertified_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                       start_date="07/2015", end_date="09/2015",
                                                                       is_quarter=True, number_of_errors=0)

            cls.test_revalidate_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                      start_date="10/2015", end_date="12/2015",
                                                                      is_quarter=True, number_of_errors=0)

    def setUp(self):
        """Test set-up."""
        super(FileTests, self).setUp()
        self.login_admin_user()

    def call_file_submission(self):
        """Call the broker file submission route."""
        if not self.filesSubmitted:
            if CONFIG_BROKER["use_aws"]:
                self.filenames = {"appropriations": "test1.csv",
                                  "award_financial": "test2.csv",
                                  "program_activity": "test4.csv", "cgac_code": "SYS",
                                  "reporting_period_start_date": "01/2001",
                                  "reporting_period_end_date": "03/2001", "is_quarter": True}
            else:
                # If local must use full destination path
                file_path = CONFIG_BROKER["broker_files"]
                self.filenames = {"appropriations": os.path.join(file_path, "test1.csv"),
                                  "award_financial": os.path.join(file_path, "test2.csv"),
                                  "program_activity": os.path.join(file_path, "test4.csv"), "cgac_code": "SYS",
                                  "reporting_period_start_date": "01/2001",
                                  "reporting_period_end_date": "03/2001", "is_quarter": True}
            self.submitFilesResponse = self.app.post_json("/v1/submit_files/", self.filenames,
                                                          headers={"x-session-id": self.session_id})
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
        self.assertIn(CONFIG_BROKER["award_file_name"], json["award_key"])
        self.assertIn("test4.csv", json["program_activity_key"])
        self.assertIn("credentials", json)

        credentials = json["credentials"]
        for requiredField in ("AccessKeyId", "SecretAccessKey", "SessionToken", "SessionToken"):
            self.assertIn(requiredField, credentials)
            self.assertTrue(len(credentials[requiredField]))

        self.assertIn("bucket_name", json)
        self.assertTrue(len(json["bucket_name"]))

        file_results = self.upload_file_by_url("/" + json["appropriations_key"], "test1.csv")
        self.assertGreater(file_results['bytesWritten'], 0)

        # Test that job ids are returned
        response_dict = json
        file_keys = ["program_activity", "award_financial", "appropriations"]
        with create_app().app_context():
            sess = GlobalDB.db().session
            for key in file_keys:
                id_key = '{}_id'.format(key)
                self.assertIn(id_key, response_dict)
                job_id = response_dict[id_key]
                self.assertIsInstance(job_id, int)
                # Check that original filenames were stored in DB
                original_filename = sess.query(Job).filter(Job.job_id == job_id).one().original_filename
                self.assertEquals(original_filename, self.filenames[key])
            # check that submission got mapped to the correct user
            submission_id = response_dict["submission_id"]
            self.file_submission_id = submission_id
            submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
        self.assertEqual(submission.user_id, self.submission_user_id)
        # Check that new submission is unpublished
        self.assertEqual(submission.publish_status_id, PUBLISH_STATUS_DICT['unpublished'])

        # Call upload complete route
        finalize_response = self.check_upload_complete(response_dict["appropriations_id"])
        self.assertEqual(finalize_response.status_code, 200)

    def test_update_submission(self):
        """ Test submit_files with an existing submission ID """
        self.call_file_submission()
        # note: this is a quarterly test submission, so
        # updated dates must still reflect a quarter
        if CONFIG_BROKER["use_aws"]:
            update_json = {"existing_submission_id": self.updateSubmissionId,
                           "award_financial": "updated.csv",
                           "reporting_period_start_date": "04/2016",
                           "reporting_period_end_date": "06/2016"}
        else:
            # If local must use full destination path
            file_path = CONFIG_BROKER["broker_files"]
            update_json = {"existing_submission_id": self.updateSubmissionId,
                           "award_financial": os.path.join(file_path, "updated.csv"),
                           "reporting_period_start_date": "04/2016",
                           "reporting_period_end_date": "06/2016"}
        # Mark submission as published
        with create_app().app_context():
            sess = GlobalDB.db().session
            update_submission = sess.query(Submission).filter(Submission.submission_id == self.updateSubmissionId).one()
            update_submission.publish_status_id = PUBLISH_STATUS_DICT['published']
            sess.commit()
            update_response = self.app.post_json("/v1/submit_files/", update_json,
                                                 headers={"x-session-id": self.session_id})
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_response.headers.get("Content-Type"), "application/json")

            json = update_response.json
            self.assertIn("updated.csv", json["award_financial_key"])
            submission_id = json["submission_id"]
            submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
            self.assertEqual(submission.cgac_code, "SYS")  # Should not have changed agency name
            self.assertEqual(submission.reporting_start_date.strftime("%m/%Y"), "04/2016")
            self.assertEqual(submission.reporting_end_date.strftime("%m/%Y"), "06/2016")
            self.assertEqual(submission.publish_status_id, PUBLISH_STATUS_DICT['updated'])

    def test_bad_quarter_or_month(self):
        """ Test file submissions for Q5, 13, and AB, and year of ABCD """
        update_json = {
            "cgac_code": "020",
            "is_quarter": True,
            "award_financial": "updated.csv",
            "reporting_period_start_date": "12/2016",
            "reporting_period_end_date": "13/2016"}
        update_response = self.app.post_json("/v1/submit_files/", update_json,
                                             headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn("Date must be provided as", update_response.json["message"])

        update_json = {
            # make sure date checks work as expected for an existing submission
            "existing_submission_id": self.status_check_submission_id,
            "award_financial": "updated.csv",
            "reporting_period_start_date": "AB/2016",
            "reporting_period_end_date": "CD/2016"}
        update_response = self.app.post_json("/v1/submit_files/", update_json,
                                             headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn("Date must be provided as", update_response.json["message"])

        update_json = {
            "cgac_code": "020",
            "is_quarter": True,
            "award_financial": "updated.csv",
            "reporting_period_start_date": "Q1/ABCD",
            "reporting_period_end_date": "Q2/2016"}
        update_response = self.app.post_json("/v1/submit_files/", update_json,
                                             headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn("Date must be provided as", update_response.json["message"])

    def test_check_status_no_login(self):
        """ Test response with no login """
        self.logout()
        post_json = {"submission_id": self.status_check_submission_id}
        response = self.app.post_json("/v1/check_status/", post_json, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_no_session_id(self):
        """ Test response with no session ID """
        post_json = {"submission_id": self.status_check_submission_id}
        response = self.app.post_json("/v1/check_status/", post_json, expect_errors=True)
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_permission(self):
        """ Test that other users do not have access to status check submission """
        post_json = {"submission_id": self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        # Call check status route
        response = self.app.post_json("/v1/check_status/", post_json, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 403)

    def test_check_status_admin(self):
        """ Test that admins have access to other user's submissions """
        post_json = {"submission_id": self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        # Call check status route (also checking case insensitivity of header here)
        response = self.app.post_json("/v1/check_status/", post_json, expect_errors=True,
                                      headers={"x-SESSION-id": self.session_id})
        # Assert 200 status
        self.assertEqual(response.status_code, 200)

    def test_check_status(self):
        """Test broker status route response."""
        post_json = {"submission_id": self.status_check_submission_id}
        # Populating error info before calling route to avoid changing last update time

        with create_app().app_context():
            sess = GlobalDB.db().session
            populate_submission_error_info(self.status_check_submission_id)

            response = self.app.post_json("/v1/check_status/", post_json, headers={"x-session-id": self.session_id})

            self.assertEqual(response.status_code, 200, msg=str(response.json))
            self.assertEqual(response.headers.get("Content-Type"), "application/json")
            json = response.json
            # response ids are coming back as string, so patch the jobIdDict
            job_id_dict = {k: str(self.jobIdDict[k]) for k in self.jobIdDict.keys()}
            job_list = json["jobs"]
            approp_job = None
            cross_job = None
            for job in job_list:
                if str(job["job_id"]) == str(job_id_dict["appropriations"]):
                    # Found the job to be checked
                    approp_job = job
                elif str(job["job_id"]) == str(job_id_dict["cross_file"]):
                    # Found cross file job
                    cross_job = job

            # Must have an approp job and cross-file job
            self.assertNotEqual(approp_job, None)
            self.assertNotEqual(cross_job, None)
            # And that job must have the following
            self.assertEqual(approp_job["job_status"], "ready")
            self.assertEqual(approp_job["job_type"], "csv_record_validation")
            self.assertEqual(approp_job["file_type"], "appropriations")
            self.assertEqual(approp_job["filename"], "approp.csv")
            self.assertEqual(approp_job["file_status"], "complete")
            self.assertIn("missing_header_one", approp_job["missing_headers"])
            self.assertIn("missing_header_two", approp_job["missing_headers"])
            self.assertIn("duplicated_header_one", approp_job["duplicated_headers"])
            self.assertIn("duplicated_header_two", approp_job["duplicated_headers"])
            # Check file size and number of rows
            self.assertEqual(approp_job["file_size"], 2345)
            self.assertEqual(approp_job["number_of_rows"], 567)
            self.assertEqual(approp_job["error_type"], "row_errors")

            # Check error metadata for specified error
            rule_error_data = None
            for data in approp_job["error_data"]:
                if data["field_name"] == "header_three":
                    rule_error_data = data
            self.assertIsNotNone(rule_error_data)
            self.assertEqual(rule_error_data["field_name"], "header_three")
            self.assertEqual(rule_error_data["error_name"], "rule_failed")
            self.assertEqual(rule_error_data["error_description"], "A rule failed for this value")
            self.assertEqual(rule_error_data["occurrences"], "7")
            self.assertEqual(rule_error_data["rule_failed"], "Header three value must be real")
            self.assertEqual(rule_error_data["original_label"], "A1")
            # Check warning metadata for specified warning
            warning_error_data = None
            for data in approp_job["warning_data"]:
                if data["field_name"] == "header_three":
                    warning_error_data = data
            self.assertIsNotNone(warning_error_data)
            self.assertEqual(warning_error_data["field_name"], "header_three")
            self.assertEqual(warning_error_data["error_name"], "rule_failed")
            self.assertEqual(warning_error_data["error_description"], "A rule failed for this value")
            self.assertEqual(warning_error_data["occurrences"], "7")
            self.assertEqual(warning_error_data["rule_failed"], "Header three value looks odd")
            self.assertEqual(warning_error_data["original_label"], "A2")

            rule_error_data = None
            for data in cross_job["error_data"]:
                if data["field_name"] == "header_four":
                    rule_error_data = data

            self.assertEqual(rule_error_data["source_file"], "appropriations")
            self.assertEqual(rule_error_data["target_file"], "award")

            # Check submission metadata
            self.assertEqual(json["cgac_code"], "SYS")
            self.assertEqual(json["reporting_period_start_date"], "Q1/2016")
            self.assertEqual(json["reporting_period_end_date"], "Q1/2016")

            # Check submission level info
            self.assertEqual(json["number_of_errors"], 17)
            self.assertEqual(json["number_of_rows"], 667)

            # Get submission from db for attribute checks
            submission = sess.query(Submission).filter(
                Submission.submission_id == self.status_check_submission_id).one()

            # Check number of errors and warnings in submission table
            self.assertEqual(submission.number_of_errors, 17)
            self.assertEqual(submission.number_of_warnings, 7)

            # Check that submission was created today, this test may fail if run right at midnight UTC
            self.assertEqual(json["created_on"], datetime.utcnow().strftime("%m/%d/%Y"))
            self.assertEqual(json["last_updated"], submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S"))

    def test_get_obligations(self):
        submission = SubmissionFactory()
        self.session.add(submission)
        self.session.commit()
        response = self.app.post_json("/v1/get_obligations/", {"submission_id": submission.submission_id},
                                      headers={"x-session-id": self.session_id})
        assert response.status_code == 200
        assert "total_obligations" in response.json

    def test_get_protected_files(self):
        """ Check get_protected_files route """

        if CONFIG_BROKER["use_aws"]:
            response = self.app.get("/v1/get_protected_files/", headers={"x-session-id": self.session_id})
            self.assertEqual(response.status_code, 200, msg=str(response.json))
            self.assertEqual(response.headers.get("Content-Type"), "application/json")
            json = response.json
            self.assertNotEqual(len(json["urls"]), 0)
        else:
            response = self.app.get("/v1/get_protected_files/",
                                    headers={"x-session-id": self.session_id}, expect_errors=True)
            self.assertEqual(response.status_code, 400, msg=str(response.json))
            self.assertEqual(response.headers.get("Content-Type"), "application/json")
            json = response.json
            self.assertEqual(json["urls"], {})

    def check_upload_complete(self, job_id):
        """Check status of a broker file submission."""
        post_json = {"upload_id": job_id}
        return self.app.post_json("/v1/finalize_job/", post_json, headers={"x-session-id": self.session_id})

    @staticmethod
    def upload_file_by_url(s3_file_name, filename):
        """Upload file and return filename and bytes written."""
        full_path = os.path.join(CONFIG_BROKER['path'], "tests", "integration", "data", filename)

        if CONFIG_BROKER['local']:
            # If not using AWS, put file submission in location
            # specified by the config file
            broker_file_path = CONFIG_BROKER['broker_files']
            copy(full_path, broker_file_path)
            submitted_file = os.path.join(broker_file_path, filename)
            return {'bytesWritten': os.path.getsize(submitted_file), 's3FileName': full_path}
        else:
            # Use boto to put files on S3
            s3conn = boto.s3.connect_to_region(CONFIG_BROKER["aws_region"])
            bucket_name = CONFIG_BROKER['aws_bucket']
            key = Key(s3conn.get_bucket(bucket_name))
            key.key = s3_file_name
            bytes_written = key.set_contents_from_filename(full_path)
            return {'bytesWritten': bytes_written, 's3FileName': s3_file_name}

    def test_error_report(self):
        """Test broker csv_validation error report."""
        post_json = {"submission_id": self.error_report_submission_id}
        response = self.app.post_json(
            "/v1/submission_error_reports/", post_json, headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 14)
        self.assertIn("cross_appropriations-program_activity", response.json)

    def test_warning_reports(self):
        """Test broker csv_validation error report."""
        post_json = {"submission_id": self.error_report_submission_id}
        response = self.app.post_json(
            "/v1/submission_warning_reports/", post_json, headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        self.assertEqual(len(response.json), 14)
        self.assertIn("cross_warning_appropriations-program_activity", response.json)

    def check_metrics(self, submission_id, exists, type_file):
        """Get error metrics for specified submission."""
        post_json = {"submission_id": submission_id}
        response = self.app.post_json("/v1/error_metrics/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)

        type_file_length = len(response.json[type_file])
        if exists:
            self.assertGreater(type_file_length, 0)
        else:
            self.assertEqual(type_file_length, 0)

    def test_metrics(self):
        """Test broker status record handling."""
        # Check the route
        self.check_metrics(self.test_metrics_submission_id, False, "award")
        self.check_metrics(self.test_metrics_submission_id, True, "award_financial")
        self.check_metrics(self.test_metrics_submission_id, True, "appropriations")

    def test_file_generation(self):
        """ Test the generate and check routes for external files """
        # For file generation submission, call generate route for D1 and check results
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json

        # use_aws is true when the PR unit tests run so the date range specified returns no results.
        # checking is in place for "failed" until use_aws is flipped to false
        self.assertIn(json["status"], ["failed", "waiting", "finished"])
        self.assertEqual(json["file_type"], "D1")
        self.assertIn("url", json)
        self.assertEqual(json["start"], "01/02/2016")
        self.assertEqual(json["end"], "02/03/2016")

        # this is to accommodate for checking for the "failed" status
        self.assertIn(json["message"], ["", "D1 data unavailable for the specified date range"])

        # Then call check generation route for D2, E and F and check results
        post_json = {"submission_id": self.generation_submission_id, "file_type": "E"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "finished")
        self.assertEqual(json["file_type"], "E")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "")

        post_json = {"submission_id": self.generation_submission_id, "file_type": "D2"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "D2")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "Generated file had file-level errors")

        post_json = {"submission_id": self.generation_submission_id, "file_type": "F"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "F")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "File was invalid")

        # Test permission error
        self.login_user()
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 403)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "D1")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["start"], "")
        self.assertEqual(json["end"], "")
        self.assertEqual(json["message"], "User does not have permission to view that submission")

    def test_detached_file_generation(self):
        """ Test the generate and check routes for external files """
        # For file generation submission, call generate route for D1 and check results
        post_json = {'file_type': 'D1', 'start': '01/02/2016', 'end': '02/03/2016', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertIn(json["status"], ["running", "finished"])
        self.assertEqual(json["file_type"], "D1")
        self.assertIn("url", json)
        self.assertEqual(json["start"], "01/02/2016")
        self.assertEqual(json["end"], "02/03/2016")
        self.assertEqual(json["message"], "")
        self.assertIsNotNone(json["job_id"])

        # call check generation status route for D2 and check results
        post_json = {}
        response = self.app.post_json("/v1/check_detached_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        assert response.json['message'] == (
            'job_id: Missing data for required field.')

        post_json = {'job_id': -1}
        response = self.app.post_json("/v1/check_detached_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        json = response.json
        self.assertEqual(json["message"], 'No generation job found with the specified ID')

    def test_delete_submission(self):
        sess = GlobalDB.db().session
        jobs_orig = sess.query(Job).filter(Job.submission_id == self.test_delete_submission_id).all()
        job_ids = [job.job_id for job in jobs_orig]

        post_json = {'submission_id': self.test_delete_submission_id}
        response = self.app.post_json("/v1/delete_submission/", post_json, headers={"x-session-id": self.session_id})
        self.assertEqual(response.json["message"], "Success")

        response = self.app.post_json("/v1/check_status/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json["message"], "No such submission")

        # check if models were actually delete (verifying cascading worked)
        jobs_new = sess.query(Job).filter(Job.submission_id == self.test_delete_submission_id).all()
        self.assertEqual(jobs_new, [])

        job_deps = sess.query(JobDependency).filter(or_(JobDependency.job_id.in_(job_ids),
                                                        JobDependency.prerequisite_id.in_(job_ids))).all()
        self.assertEqual(job_deps, [])

        # test trying to delete a certified submission (failure expected)
        post_json = {'submission_id': self.test_certified_submission_id}
        response = self.app.post_json("/v1/delete_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json["message"], "Certified submissions cannot be deleted")

    def test_certify_submission(self):
        post_json = {'submission_id': self.test_uncertified_submission_id}
        response = self.app.post_json("/v1/certify_submission/", post_json, headers={"x-session-id": self.session_id})
        self.assertEqual(response.json['message'], "Success")

        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json("/v1/certify_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], "Submission cannot be certified due to critical errors")

    def test_revalidate_submission(self):
        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json("/v1/restart_validation/", post_json,
                                      headers={"x-session-id": self.session_id})
        self.assertEqual(response.json['message'], "Success")

    @staticmethod
    def insert_submission(sess, submission_user_id, cgac_code=None, start_date=None, end_date=None,
                          is_quarter=False, number_of_errors=0, publish_status_id=1):
        """Insert one submission into job tracker and get submission ID back."""
        publishable = True if number_of_errors == 0 else False
        sub = Submission(created_at=datetime.utcnow(),
                         user_id=submission_user_id,
                         cgac_code=cgac_code,
                         reporting_start_date=datetime.strptime(start_date, '%m/%Y'),
                         reporting_end_date=datetime.strptime(end_date, '%m/%Y'),
                         is_quarter_format=is_quarter,
                         number_of_errors=number_of_errors,
                         publish_status_id=publish_status_id,
                         publishable=publishable)
        sess.add(sub)
        sess.commit()
        return sub.submission_id

    @staticmethod
    def insert_job(sess, filetype, status, type_id, submission, job_id=None, filename=None,
                   file_size=None, num_rows=None):
        """Insert one job into job tracker and get ID back."""
        job = Job(
            file_type_id=filetype,
            job_status_id=status,
            job_type_id=type_id,
            submission_id=submission,
            original_filename=filename,
            file_size=file_size,
            number_of_rows=num_rows
        )
        if job_id:
            job.job_id = job_id
        sess.add(job)
        sess.commit()
        return job

    @staticmethod
    def insert_file(sess, job_id, status):
        """Insert one file into error database and get ID back."""
        fs = File(job_id=job_id, filename=' ', file_status_id=status)
        sess.add(fs)
        sess.commit()
        return fs.file_id

    @classmethod
    def insert_row_level_error(cls, sess, job_id):
        """Insert one error into error database."""
        ed = ErrorMetadata(
            job_id=job_id,
            filename='test.csv',
            field_name='header 1',
            error_type_id=ERROR_TYPE_DICT['type_error'],
            occurrences=100,
            first_row=123,
            rule_failed='Type Check'
        )
        sess.add(ed)
        sess.commit()
        return ed.error_metadata_id

    @classmethod
    def setup_file_generation_submission(cls, sess, submission_id=None):
        """Create jobs for D, E, and F files."""
        submission_id = cls.generation_submission_id if not submission_id else submission_id
        submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()

        # Create D1 jobs ready for generation route to be called
        cls.insert_job(
            sess,
            FILE_TYPE_DICT['award_procurement'],
            JOB_STATUS_DICT['ready'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        award_roc_val_job = cls.insert_job(
            sess,
            FILE_TYPE_DICT['award_procurement'],
            JOB_STATUS_DICT['waiting'],
            JOB_TYPE_DICT['csv_record_validation'],
            submission.submission_id
        )
        # Create E and F jobs ready for check route
        awardee_att_job = cls.insert_job(
            sess,
            FILE_TYPE_DICT['awardee_attributes'],
            JOB_STATUS_DICT['finished'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        sub_award_job = cls.insert_job(
            sess,
            FILE_TYPE_DICT['sub_award'],
            JOB_STATUS_DICT['invalid'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        sub_award_job.error_message = "File was invalid"

        # Create D2 jobs
        cls.insert_job(
            sess,
            FILE_TYPE_DICT['award'],
            JOB_STATUS_DICT['finished'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        cls.insert_job(
            sess,
            FILE_TYPE_DICT['award'],
            JOB_STATUS_DICT['invalid'],
            JOB_TYPE_DICT['csv_record_validation'],
            submission.submission_id
        )
        # Create dependency
        awardee_att_dep = JobDependency(
            job_id=awardee_att_job.job_id,
            prerequisite_id=award_roc_val_job.job_id
        )
        sess.add(awardee_att_dep)
        sess.commit()

    @classmethod
    def setup_submission_with_error(cls, sess, row_error_submission_id):
        """ Set up a submission that will come back with a status of validation_errors """
        job_values = {
            'awardFin': [3, 4, 2, "awardFin.csv", 100, 100],
            'appropriations': [1, 4, 2, "approp.csv", 2345, 567],
            'program_activity': [2, 4, 2, "programActivity.csv", None, None],
            'cross_file': [None, 4, 4, 2, None, None, None]
        }

        for job_key, values in job_values.items():
            job = FileTests.insert_job(
                sess,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=row_error_submission_id,
                filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
        # Add errors to cross file job
        metadata = ErrorMetadata(
            job_id=job.job_id,
            occurrences=2,
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        sess.add(metadata)
        sess.commit()

    @classmethod
    def setup_jobs_for_status_check(cls, sess, submission_id):
        """Set up test jobs for job status test."""
        job_values = {
            'uploadFinished': [FILE_TYPE_DICT['award'], JOB_STATUS_DICT['finished'],
                               JOB_TYPE_DICT['file_upload'], None, None, None],
            'recordRunning': [FILE_TYPE_DICT['award'], JOB_STATUS_DICT['running'],
                              JOB_TYPE_DICT['csv_record_validation'], None, None, None],
            'externalWaiting': [FILE_TYPE_DICT['award'], JOB_STATUS_DICT['waiting'],
                                JOB_TYPE_DICT['external_validation'], None, None, None],
            'awardFin': [FILE_TYPE_DICT['award_financial'], JOB_STATUS_DICT['ready'],
                         JOB_TYPE_DICT['csv_record_validation'], "awardFin.csv", 100, 100],
            'appropriations': [FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['ready'],
                               JOB_TYPE_DICT['csv_record_validation'], "approp.csv", 2345, 567],
            'program_activity': [FILE_TYPE_DICT['program_activity'], JOB_STATUS_DICT['ready'],
                                 JOB_TYPE_DICT['csv_record_validation'], "programActivity.csv", None, None],
            'cross_file': [None, JOB_STATUS_DICT['finished'], JOB_TYPE_DICT['validation'], 2, None, None, None]
        }
        job_id_dict = {}

        for job_key, values in job_values.items():
            job = FileTests.insert_job(
                sess,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=submission_id,
                filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
            job_id_dict[job_key] = job.job_id

        # For appropriations job, create an entry in file for this job
        file_rec = File(
            job_id=job_id_dict["appropriations"],
            filename="approp.csv",
            file_status_id=FILE_STATUS_DICT['complete'],
            headers_missing="missing_header_one, missing_header_two",
            headers_duplicated="duplicated_header_one, duplicated_header_two")
        sess.add(file_rec)

        cross_file = File(
            job_id=job_id_dict["cross_file"],
            filename="approp.csv",
            file_status_id=FILE_STATUS_DICT['complete'],
            headers_missing="",
            headers_duplicated="")
        sess.add(cross_file)

        # Put some entries in error data for approp job
        rule_error = ErrorMetadata(
            job_id=job_id_dict["appropriations"],
            filename="approp.csv",
            field_name="header_three",
            error_type_id=ERROR_TYPE_DICT['rule_failed'],
            occurrences=7,
            rule_failed="Header three value must be real",
            original_rule_label="A1",
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        sess.add(rule_error)

        warning_error = ErrorMetadata(
            job_id=job_id_dict["appropriations"],
            filename="approp.csv",
            field_name="header_three",
            error_type_id=ERROR_TYPE_DICT['rule_failed'],
            occurrences=7,
            rule_failed="Header three value looks odd",
            original_rule_label="A2",
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['warning']
        )
        sess.add(warning_error)

        req_error = ErrorMetadata(
            job_id=job_id_dict["appropriations"],
            filename="approp.csv",
            field_name="header_four",
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed="A required value was not provided",
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        sess.add(req_error)

        cross_error = ErrorMetadata(
            job_id=job_id_dict["cross_file"],
            filename="approp.csv",
            field_name="header_four",
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed="A required value was not provided",
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        sess.add(cross_error)

        sess.commit()
        return job_id_dict

    @classmethod
    def setup_jobs_for_reports(cls, sess, error_report_submission_id):
        """Setup jobs table for checking validator unit test error reports."""
        finished = JOB_STATUS_DICT['finished']
        csv_validation = JOB_TYPE_DICT['csv_record_validation']
        FileTests.insert_job(sess, filetype=FILE_TYPE_DICT['award'], status=finished, type_id=csv_validation,
                             submission=error_report_submission_id)
        FileTests.insert_job(sess, filetype=FILE_TYPE_DICT['award_financial'], status=finished, type_id=csv_validation,
                             submission=error_report_submission_id)
        FileTests.insert_job(sess, filetype=FILE_TYPE_DICT['appropriations'], status=finished, type_id=csv_validation,
                             submission=error_report_submission_id)
        FileTests.insert_job(sess, filetype=FILE_TYPE_DICT['program_activity'], status=finished, type_id=csv_validation,
                             submission=error_report_submission_id)

    @classmethod
    def setup_file_data(cls, sess, submission_id):
        """Setup test data for the route test"""
        ready = JOB_STATUS_DICT['ready']
        csv_validation = JOB_TYPE_DICT['csv_record_validation']

        job = FileTests.insert_job(
            sess,
            filetype=FILE_TYPE_DICT['award'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # everything is fine
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['complete'])

        job = FileTests.insert_job(
            sess,
            filetype=FILE_TYPE_DICT['award_financial'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # bad header
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['unknown_error'])

        job = FileTests.insert_job(
            sess,
            filetype=FILE_TYPE_DICT['appropriations'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # validation level errors
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['complete'])
        cls.insert_row_level_error(sess, job.job_id)
