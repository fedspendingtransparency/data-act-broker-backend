import calendar
import boto
import os

from boto.s3.key import Key
from datetime import datetime
from shutil import copy

from dataactbroker.handlers.submission_handler import populate_submission_error_info

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.jobModels import Submission, Job, JobDependency, CertifyHistory, CertifiedFilesHistory
from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, ERROR_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT)

from dataactvalidator.health_check import create_app

from sqlalchemy import or_
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.integration.baseTestAPI import BaseTestAPI


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
            cls.other_user_email = other_user.email
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

            cls.test_updated_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                   start_date="07/2016", end_date="09/2016",
                                                                   is_quarter=True, number_of_errors=0,
                                                                   publish_status_id=3)

            cls.test_uncertified_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                       start_date="04/2015", end_date="06/2015",
                                                                       is_quarter=True, number_of_errors=0)

            cls.test_revalidate_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                      start_date="10/2015", end_date="12/2015",
                                                                      is_quarter=True, number_of_errors=0)

            cls.test_monthly_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                   start_date="10/2015", end_date="12/2015",
                                                                   is_quarter=False, number_of_errors=0)

            cls.test_fabs_submission_id = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                                start_date="10/2015", end_date="12/2015",
                                                                is_quarter=False, number_of_errors=0,
                                                                is_fabs=True)

            cls.test_other_user_submission_id = cls.insert_submission(sess, cls.other_user_id, cgac_code="NOT",
                                                                      start_date="10/2015", end_date="12/2015",
                                                                      is_quarter=True, number_of_errors=0)
            for job_type in ['file_upload', 'csv_record_validation']:
                for file_type in ['appropriations', 'program_activity', 'award_financial']:
                    cls.insert_job(sess, FILE_TYPE_DICT[file_type], FILE_STATUS_DICT['complete'],
                                   JOB_TYPE_DICT[job_type], cls.test_other_user_submission_id, job_id=None,
                                   filename=None, file_size=None, num_rows=None)
            cls.insert_job(sess, None, FILE_STATUS_DICT['complete'],
                           JOB_TYPE_DICT['validation'], cls.test_other_user_submission_id, job_id=None,
                           filename=None, file_size=None, num_rows=None)

            cls.test_certify_history_id = cls.setup_certification_history(sess)

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
                                  "program_activity": "test4.csv",
                                  "cgac_code": "SYS", "frec_code": None,
                                  "reporting_period_start_date": "01/2001",
                                  "reporting_period_end_date": "03/2001", "is_quarter": True}
            else:
                # If local must use full destination path
                file_path = CONFIG_BROKER["broker_files"]
                self.filenames = {"appropriations": os.path.join(file_path, "test1.csv"),
                                  "award_financial": os.path.join(file_path, "test2.csv"),
                                  "program_activity": os.path.join(file_path, "test4.csv"),
                                  "cgac_code": "SYS", "frec_code": None,
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
        # note: this is a quarterly test submission, so updated dates must still reflect a quarter
        file_path = "updated.csv" if CONFIG_BROKER["use_aws"] else os.path.join(CONFIG_BROKER["broker_files"],
                                                                                "updated.csv")
        update_json = {"existing_submission_id": self.updateSubmissionId,
                       "award_financial": file_path,
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

    def test_submit_file_certified_period(self):
        """ Test file submissions for Q4, 2015, submission w same period already been certified """
        update_json = {
            "cgac_code": "SYS",
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/submit_files/", update_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], "A submission with the same period already exists.")

    def test_submit_file_fabs_dabs_route(self):
        """ Test trying to update a FABS submission via the DABS route """
        update_json = {
            "existing_submission_id": self.test_fabs_submission_id,
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/submit_files/", update_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "Existing submission must be a DABS submission")

    def test_submit_file_new_missing_params(self):
        """ Test file submission for a new submission while missing any of the parameters """
        update_json = {
            "cgac_code": "TEST",
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/submit_files/", update_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "Must include all files for a new submission")

    def test_submit_file_old_no_params(self):
        """ Test file submission for an existing submission while not providing any file parameters """
        update_json = {
            "existing_submission_id": self.status_check_submission_id,
            "is_quarter": True,
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/submit_files/", update_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "Must include at least one file for an existing submission")

    def test_submit_file_wrong_permissions_wrong_user(self):
        self.login_user()
        new_submission_json = {
            "cgac_code": "NOT",
            "frec_code": None,
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/submit_files/", new_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['message'], "User does not have permissions to write to that agency")

    def test_submit_file_wrong_permissions_right_user(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            "existing_submission_id": self.test_other_user_submission_id,
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/submit_files/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 200)

    def test_submit_file_missing_parameters(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/submit_files/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'],
                         "Missing required parameter: cgac_code, frec_code, or existing_submission_id")

    def test_submit_file_incorrect_parameters(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            "existing_submission_id": -99,
            "is_quarter": True,
            "appropriations": "appropriations.csv",
            "award_financial": "award_financial.csv",
            "program_activity": "program_activity.csv",
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/submit_files/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'existing_submission_id must be a valid submission_id')

    def test_revalidation_threshold_no_login(self):
        """ Test response with no login """
        self.logout()
        response = self.app.get("/v1/revalidation_threshold/", None, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_revalidation_threshold(self):
        """ Test revalidation threshold route response. """
        self.login_user()
        response = self.app.get("/v1/revalidation_threshold/", None, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_metadata_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/submission_metadata/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_metadata_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        response = self.app.get("/v1/submission_metadata/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 403)

    def test_submission_metadata_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        response = self.app.get("/v1/submission_metadata/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_metadata(self):
        """ Test submission_metadata route response. """
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/submission_metadata/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

        # Make sure we got the right submission
        json = response.json
        self.assertEqual(json["cgac_code"], "SYS")
        self.assertEqual(json["reporting_period"], "Q1/2016")

    def test_submission_data_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_invalid_file_type(self):
        """ Test response with a completely invalid file type """
        self.logout()
        params = {"submission_id": self.status_check_submission_id, "type": 'approp'}
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_bad_file_type(self):
        """ Test response with a real file type requested but invalid for this submission """
        self.logout()
        params = {"submission_id": self.status_check_submission_id, "type": 'fabs'}
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 403)

    def test_submission_data_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_data_invalid_type(self):
        """ Test that an invalid file type to check status returns an error """
        params = {"submission_id": self.status_check_submission_id, "type": "approp"}
        response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "approp is not a valid file type")

    def test_submission_data_type_param(self):
        """ Test broker status route response with case-ignored type argument. """
        params = {"submission_id": self.status_check_submission_id, "type": "apPropriations"}
        response = self.app.get("/v1/submission_data/", params, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        json = response.json

        # create list of all file types including cross other than fabs
        self.assertEqual(len(json["jobs"]), 1)
        self.assertEqual(json["jobs"][0]["file_type"], "appropriations")

    def test_submission_data(self):
        """ Test submission_data route response. """
        params = {"submission_id": self.status_check_submission_id}

        # Populate error data and make sure we're getting the right contents
        with create_app().app_context():
            populate_submission_error_info(self.status_check_submission_id)
            response = self.app.get("/v1/submission_data/", params, expect_errors=True,
                                    headers={"x-session-id": self.session_id})
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

            # Check error metadata for specified error
            rule_error_data = None
            for data in approp_job["error_data"]:
                if data["field_name"] == "header_three":
                    rule_error_data = data
            self.assertIsNotNone(rule_error_data)
            self.assertEqual(rule_error_data["field_name"], "header_three")
            self.assertEqual(rule_error_data["error_name"], "rule_failed")
            self.assertEqual(rule_error_data["error_description"], "A rule failed for this value.")
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
            self.assertEqual(warning_error_data["error_description"], "A rule failed for this value.")
            self.assertEqual(warning_error_data["occurrences"], "7")
            self.assertEqual(warning_error_data["rule_failed"], "Header three value looks odd")
            self.assertEqual(warning_error_data["original_label"], "A2")

            rule_error_data = None
            for data in cross_job["error_data"]:
                if data["field_name"] == "header_four":
                    rule_error_data = data

            self.assertEqual(rule_error_data["source_file"], "appropriations")
            self.assertEqual(rule_error_data["target_file"], "award")

    def test_check_status_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/check_status/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_no_session_id(self):
        """ Test response with no session ID """
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/check_status/", params, expect_errors=True)
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        # Call check status route
        response = self.app.get("/v1/check_status/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 403)

    def test_check_status_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {"submission_id": self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        # Call check status route (also checking case insensitivity of header here)
        response = self.app.get("/v1/check_status/", params, expect_errors=True,
                                headers={"x-SESSION-id": self.session_id})
        # Assert 200 status
        self.assertEqual(response.status_code, 200)

    def test_check_status(self):
        """ Test broker status route response. """
        params = {"submission_id": self.status_check_submission_id}
        response = self.app.get("/v1/check_status/", params, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        json = response.json

        # create list of all file types including cross other than fabs
        file_type_keys = {k if k != 'fabs' else 'cross' for k in FILE_TYPE_DICT}
        response_keys = {k for k in json.keys()}
        self.assertEqual(file_type_keys, response_keys)

    def test_check_status_invalid_type(self):
        """ Test that an invalid file type to check status returns an error """
        params = {"submission_id": self.status_check_submission_id, "type": "approp"}
        response = self.app.get("/v1/check_status/", params, expect_errors=True,
                                headers={"x-session-id": self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "approp is not a valid file type")

    def test_check_status_type_param(self):
        """ Test broker status route response with case-ignored type argument. """
        params = {"submission_id": self.status_check_submission_id, "type": "apPropriations"}
        response = self.app.get("/v1/check_status/", params, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        json = response.json

        # create list of all file types including cross other than fabs
        response_keys = {k for k in json.keys()}
        self.assertEqual(len(response_keys), 1)
        self.assertEqual({"appropriations"}, response_keys)

    def test_get_obligations(self):
        """ Test submission obligations with an existing Submission """
        submission = SubmissionFactory()
        self.session.add(submission)
        self.session.commit()
        response = self.app.get("/v1/get_obligations/", {"submission_id": submission.submission_id},
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

    def test_bad_file_type_check_generation_status(self):
        """ Test that an error comes back if an invalid file type is provided for check_generation_status. """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "A"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "file_type: Must be either D1, D2, E or F")

    def test_check_generation_status_finished(self):
        """ Test the check generation status route for finished generation """
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

    def test_check_generation_status_failed_file_level_errors(self):
        """ Test the check generation status route for a failed generation because of file level errors """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D2"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "D2")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "Generated file had file-level errors")

    def test_check_generation_status_failed_invalid_file(self):
        """ Test the check generation status route for a failed generation because of an invalid file """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "F"}
        response = self.app.post_json("/v1/check_generation_status/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "F")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "File was invalid")

    def test_file_generation_d1(self):
        """ Test the generate route for D1 file """
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

    def test_generate_file_invalid_file_type(self):
        """ Test invalid file type passed to generate file """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "A",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "file_type: Must be either D1, D2, E or F")

    def test_generate_file_bad_start_date_format(self):
        """ Test bad format on start date """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "ab/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "start: Must be in the format MM/DD/YYYY")

    def test_generate_file_bad_end_date_format(self):
        """ Test bad format on start date """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "ab/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "end: Must be in the format MM/DD/YYYY")

    def test_generate_file_fabs(self):
        """ Test failure while calling generate_file for a FABS submission """
        post_json = {"submission_id": self.test_fabs_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "Cannot generate files for FABS submissions")

    def test_generate_file_permission_error(self):
        """ Test permission error for generate submission """
        self.login_user()
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 403)
        json = response.json
        self.assertEqual(json["message"], "User does not have permission to access that submission")

    def test_detached_file_generation(self):
        """ Test the generate and check routes for external files """
        # For file generation submission, call generate route for D1 and check results
        post_json = {'file_type': 'D1', 'start': '01/02/2016', 'end': '02/03/2016', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertIn(json["status"], ["waiting", "running", "finished"])
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
        assert response.json['message'] == ('job_id: Missing data for required field.')

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

        response = self.app.get("/v1/check_status/", post_json, headers={"x-session-id": self.session_id},
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
        self.assertEqual(response.json["message"], "Submissions that have been certified cannot be deleted")

        # test trying to delete an updated submission (failure expected)
        post_json = {'submission_id': self.test_updated_submission_id}
        response = self.app.post_json("/v1/delete_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json["message"], "Submissions that have been certified cannot be deleted")

    def test_check_year_quarter_success(self):
        params = {'cgac_code': "SYS",
                  'submission_id': "1",
                  'reporting_fiscal_year': "2015",
                  'reporting_fiscal_period': "3"}
        response = self.app.get("/v1/check_year_quarter/", params, headers={"x-session-id": self.session_id},
                                expect_errors=False)
        self.assertEqual(response.json['message'], "Success")

    def test_check_year_quarter_already_certified(self):
        params = {'cgac_code': "SYS",
                  'submission_id': "1",
                  'reporting_fiscal_year': "2015",
                  'reporting_fiscal_period': "12"}

        response = self.app.get("/v1/check_year_quarter/", params, headers={"x-session-id": self.session_id},
                                expect_errors=True)
        self.assertEqual(response.json['message'], "A submission with the same period already exists.")
        self.assertEqual(response.json['submissionId'], self.test_certified_submission_id)

    def test_check_year_quarter_updated(self):
        params = {'cgac_code': "SYS",
                  'submission_id': "1",
                  'reporting_fiscal_year': "2016",
                  'reporting_fiscal_period': "12"}

        response = self.app.get("/v1/check_year_quarter/", params, headers={"x-session-id": self.session_id},
                                expect_errors=True)
        self.assertEqual(response.json['message'], "A submission with the same period already exists.")
        self.assertEqual(response.json['submissionId'], self.test_updated_submission_id)

    def test_certify_submission(self):
        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json("/v1/certify_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], "Submission cannot be certified due to critical errors")

        post_json = {'submission_id': self.test_monthly_submission_id}
        response = self.app.post_json("/v1/certify_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], "Monthly submissions cannot be certified")

        post_json = {'submission_id': self.test_certified_submission_id}
        response = self.app.post_json("/v1/certify_submission/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], "Submission has already been certified")

    def test_list_certifications(self):
        post_json = {'submission_id': self.test_certified_submission_id}
        response = self.app.post_json("/v1/list_certifications/", post_json, headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(len(response.json['certifications']), 0)

        post_json = {'submission_id': self.test_fabs_submission_id}
        response = self.app.post_json("/v1/list_certifications/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "FABS submissions do not have a certification history")

        post_json = {'submission_id': self.test_monthly_submission_id}
        response = self.app.post_json("/v1/list_certifications/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "This submission has no certification history")

    def test_get_certified_file(self):
        sess = GlobalDB.db().session
        certified_files_history = sess.query(CertifiedFilesHistory).\
            filter_by(certify_history_id=self.test_certify_history_id, file_type_id=FILE_TYPE_DICT["appropriations"]).\
            one()
        certified_files_history_d = sess.query(CertifiedFilesHistory). \
            filter_by(certify_history_id=self.test_certify_history_id,
                      file_type_id=FILE_TYPE_DICT["award_procurement"]). \
            one()
        certified_files_history_cross = sess.query(CertifiedFilesHistory). \
            filter_by(certify_history_id=self.test_certify_history_id,
                      file_type_id=None). \
            one()

        # valid warning file
        post_json = {'submission_id': self.test_certified_submission_id, "is_warning": True,
                     "certified_files_history_id": certified_files_history.certified_files_history_id}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id})
        self.assertIn('path/to/warning_file_a.csv', response.json['url'])
        self.assertEqual(response.status_code, 200)

        # valid uploaded file
        post_json = {'submission_id': self.test_certified_submission_id, "is_warning": False,
                     "certified_files_history_id": certified_files_history.certified_files_history_id}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id})
        self.assertIn('path/to/file_a.csv', response.json['url'])
        self.assertEqual(response.status_code, 200)

        # nonexistent certified_files_history_id
        post_json = {'submission_id': self.test_certified_submission_id, "is_warning": False,
                     "certified_files_history_id": -1}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "Invalid certified_files_history_id")

        # non-matching submission_id and certified_files_history_id
        post_json = {'submission_id': self.test_monthly_submission_id, "is_warning": False,
                     "certified_files_history_id": certified_files_history.certified_files_history_id}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'],
                         "Requested certified_files_history_id does not match submission_id provided")

        # no warning file associated with entry when requesting warning file
        post_json = {'submission_id': self.test_certified_submission_id, "is_warning": True,
                     "certified_files_history_id": certified_files_history_d.certified_files_history_id}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "History entry has no warning file")

        # no uploaded file associated with entry when requesting uploaded file
        post_json = {'submission_id': self.test_certified_submission_id, "is_warning": False,
                     "certified_files_history_id": certified_files_history_cross.certified_files_history_id}
        response = self.app.post_json("/v1/get_certified_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "History entry has no related file")

    def test_revalidate_submission(self):
        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json("/v1/restart_validation/", post_json,
                                      headers={"x-session-id": self.session_id})
        self.assertEqual(response.json['message'], "Success")

        post_json = {'submission_id': self.test_fabs_submission_id, 'd2_submission': True}
        response = self.app.post_json("/v1/restart_validation/", post_json,
                                      headers={"x-session-id": self.session_id})
        self.assertEqual(response.json['message'], "Success")

    def test_submission_report_url(self):
        """ Test that the submission's report is successfully generated """
        params = {"warning": False,
                  "file_type": "appropriations"}
        response = self.app.get("/v1/submission/{}/report_url".format(self.row_error_submission_id), params,
                                headers={"x-session-id": self.session_id}, expect_errors=False)
        self.assertEqual(response.status_code, 200)
        self.assertIn("url", response.json)

    @staticmethod
    def insert_submission(sess, submission_user_id, cgac_code=None, start_date=None, end_date=None,
                          is_quarter=False, number_of_errors=0, publish_status_id=1, is_fabs=False):
        """Insert one submission into job tracker and get submission ID back."""
        publishable = True if number_of_errors == 0 else False
        end_date = datetime.strptime(end_date, '%m/%Y')
        end_date = datetime.strptime(
                        str(end_date.year) + '/' +
                        str(end_date.month) + '/' +
                        str(calendar.monthrange(end_date.year, end_date.month)[1]),
                        '%Y/%m/%d'
                    ).date()
        sub = Submission(created_at=datetime.utcnow(),
                         user_id=submission_user_id,
                         cgac_code=cgac_code,
                         reporting_start_date=datetime.strptime(start_date, '%m/%Y'),
                         reporting_end_date=end_date,
                         is_quarter_format=is_quarter,
                         number_of_errors=number_of_errors,
                         publish_status_id=publish_status_id,
                         publishable=publishable,
                         d2_submission=is_fabs)
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
    def insert_certified_files_history(cls, sess, ch_id, submission_id, file_type=None, filename=None,
                                       warning_filename=None, narrative=None):
        """ Insert one history entry into certified files history database. """
        cfh = CertifiedFilesHistory(
            certify_history_id=ch_id,
            submission_id=submission_id,
            filename=filename,
            file_type_id=file_type,
            warning_filename=warning_filename,
            narrative=narrative
        )
        sess.add(cfh)
        sess.commit()
        return cfh.certified_files_history_id

    @classmethod
    def setup_certification_history(cls, sess):
        submission_id = cls.test_certified_submission_id

        ch = CertifyHistory(
            user_id=cls.submission_user_id,
            submission_id=submission_id
        )
        sess.add(ch)
        sess.commit()

        # Create an A file entry
        cls.insert_certified_files_history(
            sess,
            ch.certify_history_id,
            submission_id,
            FILE_TYPE_DICT["appropriations"],
            "path/to/file_a.csv",
            "path/to/warning_file_a.csv",
            "Narrative content"
        )

        # Create a D1 file entry
        cls.insert_certified_files_history(
            sess,
            ch.certify_history_id,
            submission_id,
            FILE_TYPE_DICT["award_procurement"],
            "path/to/file_d1.csv"
        )

        # Create a cross-file entry
        cls.insert_certified_files_history(
            sess,
            ch.certify_history_id,
            submission_id,
            warning_filename="path/to/cross_file.csv"
        )

        return ch.certify_history_id

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
        exec_comp_job = cls.insert_job(
            sess,
            FILE_TYPE_DICT['executive_compensation'],
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
        exec_comp_dep = JobDependency(
            job_id=exec_comp_job.job_id,
            prerequisite_id=award_roc_val_job.job_id
        )
        sess.add(exec_comp_dep)
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
            'awardFin': [FILE_TYPE_DICT['award_financial'], JOB_STATUS_DICT['ready'],
                         JOB_TYPE_DICT['csv_record_validation'], "awardFin.csv", 100, 100],
            'appropriations': [FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['ready'],
                               JOB_TYPE_DICT['csv_record_validation'], "approp.csv", 2345, 567],
            'program_activity': [FILE_TYPE_DICT['program_activity'], JOB_STATUS_DICT['ready'],
                                 JOB_TYPE_DICT['csv_record_validation'], "programActivity.csv", None, None],
            'cross_file': [None, JOB_STATUS_DICT['finished'], JOB_TYPE_DICT['validation'], 2, None, None, None]
        }
        job_id_dict = {}
        approp_job = None

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
            if job_key == 'appropriations':
                approp_job = job
            elif job_key == 'cross_file':
                cross_file_job = job

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
        approp_job.number_of_errors += 7
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
        approp_job.number_of_warnings += 7
        sess.add(warning_error)

        req_error = ErrorMetadata(
            job_id=job_id_dict["appropriations"],
            filename="approp.csv",
            field_name="header_four",
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed="This field is required for all submissions but was not provided in this row.",
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        approp_job.number_of_errors += 5
        sess.add(req_error)

        cross_error = ErrorMetadata(
            job_id=job_id_dict["cross_file"],
            filename="approp.csv",
            field_name="header_four",
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed="This field is required for all submissions but was not provided in this row.",
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        cross_file_job.number_of_errors += 5
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
