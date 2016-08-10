from baseTestAPI import BaseTestAPI
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactcore.models.jobModels import Submission, Job
from dataactcore.utils.statusCode import StatusCode

class UserTests(BaseTestAPI):
    """ Test user registration and user specific functions """

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources like submissions and jobs."""
        super(UserTests, cls).setUpClass()

        # Add submissions to one of the users
        jobDb = cls.jobTracker

        # Delete existing submissions for approved user
        jobDb.deleteSubmissionsForUserId(cls.approved_user_id)

        for i in range(0,5):
            sub = Submission(user_id = cls.approved_user_id)
            jobDb.session.add(sub)
            jobDb.session.commit()

        # Add submissions for agency user
        jobDb.deleteSubmissionsForUserId(cls.agency_user_id)
        for i in range(0,6):
            sub = Submission(user_id = cls.agency_user_id)
            sub.cgac_code = "SYS"
            jobDb.session.add(sub)
            jobDb.session.commit()
            if i == 0:
                cls.submission_id = sub.submission_id

        # Add job to first submission
        job = Job(submission_id=cls.submission_id, job_status_id=3, job_type_id=1, file_type_id=1)
        jobDb.session.add(job)
        jobDb.session.commit()
        cls.uploadId = job.job_id

    def setUp(self):
        """Test set-up."""
        super(UserTests, self).setUp()
        self.login_admin_user()

    def setUpToken(self,email):
        """Test e-mail token."""
        userDb = self.userDb
        self.registerToken = sesEmail.createToken(email, userDb, "validate_email")
        postJson = {"token": self.registerToken}
        return self.app.post_json("/v1/confirm_email_token/", postJson, headers={"x-session-id":self.session_id})

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
        self.check_response(response, StatusCode.CLIENT_ERROR)

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
        self.check_response(response, StatusCode.CLIENT_ERROR)

    def test_get_users_by_type(self):
        """Test getting user list by type."""
        agencyUsers = self.userDb.getUsersByType("agency_user")
        emails = []
        for admin in agencyUsers:
            emails.append(admin.email)
        self.assertEqual(len(agencyUsers), 14)
        for email in ["realEmail@agency.gov", "waiting@agency.gov",
            "impatient@agency.gov", "watchingPaintDry@agency.gov",
            "approved@agency.gov", "nefarious@agency.gov",]:
            self.assertIn(email, emails)
        self.assertNotIn('user@agency.gov', emails)

    def test_list_submissions(self):
        """Test listing user's submissions."""
        self.logout()
        self.login_approved_user()
        response = self.app.get("/v1/list_submissions/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertIn("submissions", response.json)
        self.assertEqual(len(response.json["submissions"]), 5)
        self.logout()

        self.login_agency_user()
        response = self.app.get("/v1/list_submissions/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertIn("submissions", response.json)
        self.assertEqual(len(response.json["submissions"]), 6)

        response = self.app.get("/v1/list_submissions/?filter_by=agency", headers={"x-session-id": self.session_id})
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
        self.check_response(response, StatusCode.LOGIN_REQUIRED, "Wrong User Type")
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
        submission = self.interfaces.jobDb.getSubmissionById(self.submission_id)
        submission.cgac_code = self.interfaces.userDb.getUserByEmail(self.test_users["approved_email"]).cgac_code
        self.interfaces.jobDb.session.commit()
        response = self.app.post_json("/v1/finalize_job/",
            postJson, expect_errors=True, headers={"x-session-id":self.session_id})
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
        userDb = self.userDb
        #make a token based on a user
        token = sesEmail.createToken(self.test_users["password_reset_email"], userDb, "validate_email")
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
        userDb = self.userDb
        user = userDb.getUserByEmail(email)
        user.user_status_id = userDb.getUserStatusId("awaiting_approval")
        userDb.session.commit()
        response = self.app.post_json("/v1/reset_password/", postJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.check_response(response, StatusCode.CLIENT_ERROR)

        user.user_status_id = userDb.getUserStatusId("approved")
        user.is_active = False
        userDb.session.commit()
        response = self.app.post_json("/v1/reset_password/", postJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.check_response(response, StatusCode.CLIENT_ERROR)

        # Test route to confirm tokens
        token = sesEmail.createToken(
            self.test_users["password_reset_email"], userDb, "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

        postJson = {"user_email": email, "password": self.user_password}
        response = self.app.post_json("/v1/set_password/", postJson, headers={"x-session-id":self.session_id})
        self.check_response(response, StatusCode.OK, "Password successfully changed")
        user = userDb.getUserByEmail(email)
        self.assertTrue(user.password_hash)

        # Call again, should error
        postJson = {"user_email": email, "password": self.user_password}
        response = self.app.post_json("/v1/set_password/", postJson, headers={"x-session-id":self.session_id}, expect_errors = True)
        self.check_response(response, StatusCode.LOGIN_REQUIRED)

    def test_check_password_token(self):
        """Test password reset with valid token."""
        userDb = self.userDb
        #make a token based on a user
        token = sesEmail.createToken(
            self.test_users["admin_email"], userDb, "password_reset")
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
        self.assertEqual(response.json["name"], "Mr. Manager")
        self.assertEqual(response.json["cgac_code"], "SYS")
        self.assertEqual(response.json["skip_guide"], False)

    def test_skip_guide(self):
        """ Set skip guide to True and check value in DB """
        self.login_approved_user()
        params = {"skip_guide":True}
        response = self.app.post_json("/v1/set_skip_guide/", params, headers={"x-session-id":self.session_id})
        self.check_response(response,StatusCode.OK,"skip_guide set successfully")
        self.assertTrue(response.json["skip_guide"])
        user = self.userDb.getUserByEmail(self.test_users['approved_email'])
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
        self.check_response(response, StatusCode.CLIENT_ERROR)

        # invalid email template
        badInput = {"users": [self.agency_user_id], "submission_id": self.submission_id,
                 "email_template": "not_a_real_template"}
        response = self.app.post_json("/v1/email_users/", badInput, expect_errors=True,
                                      headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.CLIENT_ERROR)