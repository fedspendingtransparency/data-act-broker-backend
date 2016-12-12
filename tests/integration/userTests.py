from tests.integration.baseTestAPI import BaseTestAPI
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactbroker.app import createApp
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import createUserWithPassword
from dataactcore.models.jobModels import Submission, Job
from dataactcore.models.userModel import User
from dataactcore.utils.statusCode import StatusCode
from flask_bcrypt import Bcrypt
from datetime import datetime

class UserTests(BaseTestAPI):
    """ Test user registration and user specific functions """

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

    def setUpToken(self, email):
        """Test e-mail token."""
        self.registerToken = sesEmail.createToken(email, "validate_email")
        postJson = {"token": self.registerToken}
        return self.app.post_json("/v1/confirm_email_token/", postJson, headers={"x-session-id": self.session_id})

    def test_registration_no_token(self):
        """Test without token."""
        self.logout()
        postJson = {"email": "user@agency.gov", "name": "user", "cgac_code": "SYS", "title": "title", "password": "userPass"}
        response = self.app.post_json("/v1/check_status/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.LOGIN_REQUIRED)

    def test_registration(self):
        """Test user registration."""
        self.logout()
        email = self.test_users["change_user_email"]
        self.setUpToken(email)
        postJson = {"email": email, "name": "user", "cgac_code": "SYS", "title": "title", "password": self.user_password}
        response = self.app.post_json("/v1/register/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "Registration successful")
        # Check that session does not allow another registration
        response = self.app.post_json("/v1/register/", postJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.check_response(response, StatusCode.LOGIN_REQUIRED)
        # Check that re-registration with same token is an error
        tokenJson = {"token": self.registerToken}
        self.app.post_json("/v1/confirm_email_token/", tokenJson, headers={"x-session-id":self.session_id})
        response = self.app.post_json("/v1/register/", postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.assertEqual(response.status_code,401)

    def test_registration_empty(self):
        """Test user registration with no user."""
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {}
        response = self.app.post_json("/v1/register/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR,
            "Request body must include email, name, cgac_code, title, and password")

    def test_registration_bad_email(self):
        """Test user registration with invalid email."""
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {"email": "fake@notreal.faux",
                "name": "user", "cgac_code": "SYS",
                "title":"title", "password": self.user_password}
        response = self.app.post_json("/v1/register/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(
            response, StatusCode.CLIENT_ERROR, "No users with that email")

    def test_status_change(self):
        """Test user status change."""
        status_change_user_id = self.status_change_user_id
        deniedInput = {"uid": status_change_user_id, "status": "denied"}
        approvedInput = {"uid": status_change_user_id, "status": "approved"}
        awaitingInput = {"uid": status_change_user_id, "status": "awaiting_approval"}
        emailConfirmed = {"uid": status_change_user_id, "status": "email_confirmed"}

        response = self.app.post_json("/v1/update_user/", awaitingInput, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")
        response = self.app.post_json("/v1/update_user/", approvedInput, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")
        response = self.app.post_json("/v1/update_user/", awaitingInput, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")
        response = self.app.post_json("/v1/update_user/", deniedInput, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")

        # Set back to email_confirmed for register test
        response = self.app.post_json("/v1/update_user/", emailConfirmed, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")

    def test_status_change_bad_uid(self):
        """Test status change with bad user id."""
        self.logout()
        self.login_admin_user()
        badUserId = {"uid": -100, "status": "denied"}
        response = self.app.post_json("/v1/update_user/",
            badUserId, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR, "No users with that uid")

    def test_status_change_bad_status(self):
        """Test user status change with invalid status."""
        badInput = {"uid": self.status_change_user_id, "status": "badInput"}
        response = self.app.post_json("/v1/update_user/",
            badInput, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.INTERNAL_ERROR)

    def test_list_users(self):
        """Test getting user list by status."""
        postJson = {"status": "denied"}
        response = self.app.post_json("/v1/list_users/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 1)

        response = self.app.post_json("/v1/list_users/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 16)

    def test_list_user_emails(self):
        """Test getting user emails"""
        self.logout()
        self.login_agency_user()
        response = self.app.get("/v1/list_user_emails/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 7)

    def test_list_users_bad_status(self):
        """Test getting user list with invalid status."""
        postJson = {"status": "lost"}
        response = self.app.post_json("/v1/list_users/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.INTERNAL_ERROR)

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

    def test_send_email(self):
        """Test confirm e-mail."""
        # Always use simulator to test emails!
        postJson = {"email": "success@simulator.amazonses.com"}
        response = self.app.post_json("/v1/confirm_email/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK)

    def test_check_email_token_malformed(self):
        """Test bad e-mail token."""
        postJson = {"token": "12345678"}
        response = self.app.post_json("/v1/confirm_email_token/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_check_email_token(self):
        """Test valid e-mail token."""
        #make a token based on a user
        token = sesEmail.createToken(self.test_users["password_reset_email"], "validate_email")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_email_token/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_password_reset_email(self):
        """Test password reset email."""
        self.logout()
        email = self.test_users["password_reset_email"]
        postJson = {"email": email}
        response = self.app.post_json("/v1/reset_password/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK)

        # Test password reset for unapproved user and locked user
        with createApp().app_context():
            sess = GlobalDB.db().session
            user = sess.query(User).filter(User.email == email).one()
            user.user_status_id = self.userStatusDict['awaiting_approval']
            sess.commit()
            response = self.app.post_json("/v1/reset_password/", postJson, headers={"x-session-id": self.session_id}, expect_errors=True)
            self.check_response(response, StatusCode.CLIENT_ERROR)

            user.user_status_id = self.userStatusDict['approved']
            user.is_active = False
            sess.commit()
            response = self.app.post_json("/v1/reset_password/", postJson, headers={"x-session-id": self.session_id}, expect_errors=True)
            self.check_response(response, StatusCode.CLIENT_ERROR)

            # Test route to confirm tokens
            token = sesEmail.createToken(
                self.test_users["password_reset_email"], "password_reset")
            postJson = {"token": token}
            response = self.app.post_json("/v1/confirm_password_token/", postJson, headers={"x-session-id": self.session_id})
            self.check_response(response, StatusCode.OK, "success")
            self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

            postJson = {"user_email": email, "password": self.user_password}
            response = self.app.post_json("/v1/set_password/", postJson, headers={"x-session-id": self.session_id})
            self.check_response(response, StatusCode.OK, "Password successfully changed")
            user = sess.query(User).filter(User.email == email).one()
            self.assertTrue(user.password_hash)

        # Call again, should error
        postJson = {"user_email": email, "password": self.user_password}
        response = self.app.post_json("/v1/set_password/", postJson, headers={"x-session-id": self.session_id}, expect_errors=True)
        self.check_response(response, StatusCode.LOGIN_REQUIRED)

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

    def test_update_user(self):
        """ Test user update """
        agency_user = self.agency_user_id
        input = {"uid": agency_user, "status": "approved", "is_active": False, "permissions": "agency_admin"}

        response = self.app.post_json("/v1/update_user/", input, headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK, "User successfully updated")

        badInput = {"uid": agency_user}
        response = self.app.post_json("/v1/update_user/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR)

        moreBadInput = {"status": "approved", "is_active": False, "permissions": "agency_admin"}
        response = self.app.post_json("/v1/update_user/", moreBadInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR)

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

    def test_delete_user(self):
        # Need to be an admin to delete
        self.login_admin_user()
        # Create user to be deleted, done within test to avoid interfering with tests on number of users
        email = "to_be_deleted@agency.gov"
        user_to_be_deleted = createUserWithPassword(email, "unused", Bcrypt())
        # Give this user a submission
        with createApp().app_context():
            sess = GlobalDB.db().session
            sub = SubmissionFactory(user_id = user_to_be_deleted.user_id)
            sess.add(sub)
            sess.commit()
            sub_id = sub.submission_id
        input = {"email": email}
        response = self.app.post_json("/v1/delete_user/", input, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)

        with createApp().app_context():
            sess = GlobalDB.db().session
            # Check that user is not in database
            result = sess.query(User).filter(User.user_id == user_to_be_deleted.user_id).all()
            self.assertEqual(len(result),0)
            # Check that submission has no user
            sub_after_delete = sess.query(Submission).filter(Submission.submission_id == sub_id).one()
            self.assertIsNone(sub_after_delete.user_id)
