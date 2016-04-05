from baseTest import BaseTest
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.utils.statusCode import StatusCode

class UserTests(BaseTest):
    """ Test user registration and user specific functions """

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources like submissions and jobs."""
        super(UserTests, cls).setUpClass()

        # Add submissions to one of the users
        jobDb = cls.jobTracker

        for i in range(0,5):
            sub = Submission(user_id = cls.approved_user_id)
            jobDb.session.add(sub)
            jobDb.session.commit()
            if i == 0:
                cls.submission_id = sub.submission_id

        # Add job to first submission
        job = JobStatus(submission_id = cls.submission_id,status_id = 3,type_id = 1, file_type_id = 1)
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
        token = sesEmail.createToken(email, userDb, "validate_email")
        postJson = {"token": token}
        return self.app.post_json("/v1/confirm_email_token/", postJson)

    def test_registration_no_token(self):
        """Test without token."""
        self.logout()
        postJson = {"email": "user@agency.gov", "name": "user", "agency": "agency", "title": "title", "password": "userPass"}
        response = self.app.post_json("/v1/check_status/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.LOGIN_REQUIRED)

    def test_registration(self):
        """Test user registration."""
        self.logout()
        email = self.test_users["change_user_email"]
        self.setUpToken(email)
        postJson = {"email": email, "name": "user", "agency": "agency", "title": "title", "password": self.user_password}
        response = self.app.post_json("/v1/register/", postJson)
        self.check_response(response, StatusCode.OK, "Registration successful")

    def test_registration_empty(self):
        """Test user registration with no user."""
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {}
        response = self.app.post_json("/v1/register/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.CLIENT_ERROR,
            "Request body must include email, name, agency, title, and password")

    def test_registration_bad_email(self):
        """Test user registration with invalid email."""
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {"email": "fake@notreal.faux",
                "name": "user", "agency": "agency",
                "title":"title", "password": self.user_password}
        response = self.app.post_json("/v1/register/",
            postJson, expect_errors=True)
        self.check_response(
            response, StatusCode.CLIENT_ERROR, "No users with that email")

    def test_status_change(self):
        """Test user status change."""
        status_change_user_id = self.status_change_user_id
        deniedInput = {"uid": status_change_user_id, "new_status": "denied"}
        approvedInput = {"uid": status_change_user_id, "new_status": "approved"}
        awaitingInput = {"uid": status_change_user_id, "new_status": "awaiting_approval"}
        emailConfirmed = {"uid": status_change_user_id, "new_status": "email_confirmed"}

        response = self.app.post_json("/v1/change_status/", awaitingInput)
        self.check_response(response, StatusCode.OK, "Status change successful")
        response = self.app.post_json("/v1/change_status/", approvedInput)
        self.check_response(response, StatusCode.OK, "Status change successful")
        response = self.app.post_json("/v1/change_status/", awaitingInput)
        self.check_response(response, StatusCode.OK, "Status change successful")
        response = self.app.post_json("/v1/change_status/", deniedInput)
        self.check_response(response, StatusCode.OK, "Status change successful")

        # Set back to email_confirmed for register test
        response = self.app.post_json("/v1/change_status/", emailConfirmed)
        self.check_response(response, StatusCode.OK, "Status change successful")

    def test_status_change_bad_uid(self):
        """Test status change with bad user id."""
        self.logout()
        self.login_admin_user()
        badUserId = {"uid": -100, "new_status": "denied"}
        response = self.app.post_json("/v1/change_status/",
            badUserId, expect_errors=True)
        self.check_response(response, StatusCode.CLIENT_ERROR, "No users with that uid")

    def test_status_change_bad_status(self):
        """Test user status change with invalid status."""
        badInput = {"uid": self.status_change_user_id, "new_status": "badInput"}
        response = self.app.post_json("/v1/change_status/",
            badInput, expect_errors=True)
        self.check_response(response, StatusCode.CLIENT_ERROR)

    def test_list_users(self):
        """Test getting user list by status."""
        postJson = {"status": "denied"}
        response = self.app.post_json("/v1/list_users_with_status/", postJson)
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 1)

    def test_list_users_bad_status(self):
        """Test getting user list with invalid status."""
        postJson = {"status": "lost"}
        response = self.app.post_json("/v1/list_users_with_status/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.CLIENT_ERROR)

    def test_get_users_by_type(self):
        """Test getting user list by type."""
        agencyUsers = self.userDb.getUsersByType("agency_user")
        emails = []
        for admin in agencyUsers:
            emails.append(admin.email)
        self.assertEqual(len(agencyUsers), 11)
        for email in ["realEmail@agency.gov", "waiting@agency.gov",
            "impatient@agency.gov", "watchingPaintDry@agency.gov",
            "approved@agency.gov", "nefarious@agency.gov",]:
            self.assertIn(email, emails)
        self.assertNotIn('user@agency.gov', emails)

    def test_list_submissions(self):
        """Test listing user's submissions."""
        self.logout()
        self.login_approved_user()
        response = self.app.get("/v1/list_submissions/")
        self.check_response(response, StatusCode.OK)
        self.assertIn("submission_id_list", response.json)
        self.assertEqual(len(response.json["submission_id_list"]), 5)
        self.logout()

    def test_list_users_with_status_non_admin(self):
        """Test requesting user list from a non-admin account."""
        self.login_approved_user()
        postJson = {"status": "awaiting_approval"}
        response = self.app.post_json("/v1/list_users_with_status/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.LOGIN_REQUIRED, "Wrong User Type")
        self.logout()

    def test_finalize_wrong_user(self):
        """Test finalizing a job as the wrong user."""
        # Jobs were submitted with the id for "approved user," so lookup
        # as "admin user" should fail.
        postJson = {"upload_id": self.uploadId}
        response = self.app.post_json("/v1/finalize_job/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.CLIENT_ERROR, "Cannot finalize a job created by a different user")
        self.logout()

    def test_send_email(self):
        """Test confirm e-mail."""
        # Always use simulator to test emails!
        postJson = {"email": "success@simulator.amazonses.com"}
        response = self.app.post_json("/v1/confirm_email/", postJson)
        self.check_response(response, StatusCode.OK)

    def test_check_email_token_malformed(self):
        """Test bad e-mail token."""
        postJson = {"token": "12345678"}
        response = self.app.post_json("/v1/confirm_email_token/",
            postJson, expect_errors=True)
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_check_email_token(self):
        """Test valid e-mail token."""
        userDb = self.userDb
        #make a token based on a user
        token = sesEmail.createToken(self.test_users["password_reset_email"], userDb, "validate_email")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_email_token/", postJson)
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_password_reset_email(self):
        """Test password reset email."""
        self.logout()
        email = self.test_users["password_reset_email"]
        postJson = {"email": email}
        response = self.app.post_json("/v1/reset_password/", postJson)
        self.check_response(response, StatusCode.OK)

        userDb = self.userDb
        token = sesEmail.createToken(
            self.test_users["password_reset_email"], userDb, "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson)
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

        postJson = {"user_email": email, "password": self.user_password}
        response = self.app.post_json("/v1/set_password/", postJson)
        self.check_response(response, StatusCode.OK, "Password successfully changed")
        user = userDb.getUserByEmail(email)
        self.assertTrue(user.password_hash)

    def test_check_password_token(self):
        """Test password reset with valid token."""
        userDb = self.userDb
        #make a token based on a user
        token = sesEmail.createToken(
            self.test_users["admin_email"], userDb, "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson)
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_check_bad_password_token(self):
        """Test password reset with invalid token."""
        badToken = {"token": "2345"}
        response = self.app.post_json("/v1/confirm_password_token/",
            badToken, expect_errors=True)
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_current_user(self):
        """Test retrieving current user information."""
        response = self.app.get("/v1/current_user/")
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.json["name"], "Mr. Manager")
        self.assertEqual(response.json["agency"], "Unknown")
