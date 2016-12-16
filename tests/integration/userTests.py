from tests.integration.baseTestAPI import BaseTestAPI
from dataactbroker.app import createApp
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission, Job
from dataactcore.models.userModel import User
from dataactcore.utils.statusCode import StatusCode
from datetime import datetime

class UserTests(BaseTestAPI):
    """ Test user specific functions """

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources like submissions and jobs."""
        super(UserTests, cls).setUpClass()

        with createApp().app_context():
            sess = GlobalDB.db().session

            # Add submissions to one of the users

            # Delete existing submissions for approved user
            sess.query(Submission).filter(Submission.user_id == cls.approved_user_id).delete()
            sess.commit()

            for i in range(0, 5):
                sub = Submission(user_id=cls.approved_user_id)
                sub.reporting_start_date = datetime(2015, 10, 1)
                sub.reporting_end_date = datetime(2015, 12, 31)
                sess.add(sub)
            sess.commit()

            # Add submissions for agency user
            sess.query(Submission).filter(Submission.user_id == cls.agency_user_id).delete()
            sess.commit()
            for i in range(0, 6):
                sub = Submission(user_id=cls.agency_user_id)
                sub.reporting_start_date = datetime(2015, 10, 1)
                sub.reporting_end_date = datetime(2015, 12, 31)
                sub.cgac_code = "SYS"
                sess.add(sub)
                sess.commit()
                if i == 0:
                    cls.submission_id = sub.submission_id

            # Add job to first submission
            job = Job(
                submission_id=cls.submission_id,
                job_status_id=cls.jobStatusDict['running'],
                job_type_id=cls.jobTypeDict['file_upload'],
                file_type_id=cls.fileTypeDict['appropriations']
            )
            sess.add(job)
            sess.commit()
            cls.uploadId = job.job_id

    def setUp(self):
        """Test set-up."""
        super(UserTests, self).setUp()
        self.login_admin_user()

    def test_list_user_emails(self):
        """Test getting user emails"""
        self.logout()
        self.login_agency_user()
        response = self.app.get("/v1/list_user_emails/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 7)

    def test_list_submissions(self):
        """Test listing user's submissions. The expected values here correspond to the number of submissions within
         the agency of the user that is logged in """
        self.logout()
        self.login_approved_user()
        response = self.app.get("/v1/list_submissions/?certified=mixed", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertIn("submissions", response.json)
        self.assertEqual(len(response.json["submissions"]), 1)
        self.logout()

        self.login_agency_user()
        response = self.app.get("/v1/list_submissions/?certified=mixed", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertIn("submissions", response.json)
        self.assertEqual(len(response.json["submissions"]), 5)

        response = self.app.get("/v1/list_submissions/?certified=mixed", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertIn("submissions", response.json)
        self.assertEqual(len(response.json["submissions"]), 5)
        self.logout()

    def test_list_users_with_status_non_admin(self):
        """Test requesting user list from a non-admin account."""
        self.login_approved_user()
        postJson = {"status": "awaiting_approval"}
        response = self.app.post_json("/v1/list_users_with_status/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.LOGIN_REQUIRED, "You are not authorized to perform the requested task. Please contact your administrator.")
        self.logout()

    def test_finalize_wrong_user(self):
        """Test finalizing a job as the wrong user."""
        # Jobs were submitted with the id for "approved user," so lookup
        # as "admin user" should fail.
        self.logout()
        self.login_approved_user()
        postJson = {"upload_id": self.uploadId}
        response = self.app.post_json("/v1/finalize_job/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR, "Cannot finalize a job for a different agency")
        # Give submission this user's cgac code
        with createApp().app_context():
            sess = GlobalDB.db().session
            submission = sess.query(Submission).filter(Submission.submission_id == self.submission_id).one()
            submission.cgac_code = sess.query(User).filter(User.email == self.test_users['approved_email']).one().cgac_code
            sess.commit()
        response = self.app.post_json("/v1/finalize_job/",
            postJson, expect_errors=True, headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.logout()

    def test_check_password_token(self):
        """Test password reset with valid token."""
        #make a token based on a user
        token = sesEmail.createToken(
            self.test_users["admin_email"], "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_check_bad_password_token(self):
        """Test password reset with invalid token."""
        badToken = {"token": "2345"}
        response = self.app.post_json("/v1/confirm_password_token/",
            badToken, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_current_user(self):
        """Test retrieving current user information."""
        response = self.app.get("/v1/current_user/", headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK)
        assert response.json["name"] == "Mr. Manager"
        assert response.json["cgac_code"] == "SYS"
        assert response.json["skip_guide"] == False
        assert response.json["website_admin"] == True

    def test_skip_guide(self):
        """ Set skip guide to True and check value in DB """
        self.login_approved_user()
        params = {"skip_guide": True}
        response = self.app.post_json("/v1/set_skip_guide/", params, headers={"x-session-id": self.session_id})
        self.check_response(response,StatusCode.OK, "skip_guide set successfully")
        self.assertTrue(response.json["skip_guide"])
        with createApp().app_context():
            sess = GlobalDB.db().session
            user = sess.query(User).filter(User.email == self.test_users['approved_email']).one()
        self.assertTrue(user.skip_guide)

    def test_email_users(self):
        """ Test email users """
        self.login_approved_user()
        input = {"users": [self.agency_user_id], "submission_id": self.submission_id,
                 "email_template": "review_submission"}
        response = self.app.post_json("/v1/email_users/", input, headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK, "Emails successfully sent")

        # missing request params
        badInput = {"users": [self.agency_user_id]}
        response = self.app.post_json("/v1/email_users/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR)

        # invalid submission id
        badInput = {"users": [self.agency_user_id], "submission_id": -1}
        response = self.app.post_json("/v1/email_users/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR)

        # invalid user id
        badInput = {"users": [-1], "submission_id": self.submission_id,
                 "email_template": "review_submission"}
        response = self.app.post_json("/v1/email_users/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.INTERNAL_ERROR)

        # invalid email template
        badInput = {"users": [self.agency_user_id], "submission_id": self.submission_id,
                 "email_template": "not_a_real_template"}
        response = self.app.post_json("/v1/email_users/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.INTERNAL_ERROR)
