import json
from baseTest import BaseTest
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.clearJobs import clearJobs
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.userModel import AccountType
from dataactcore.utils.statusCode import StatusCode
from flask.ext.bcrypt import Bcrypt
class UserTests(BaseTest):
    """ Test user registration and user specific functions """
    uploadId = None # set in setup function, used for testing wrong user on finalize
    CONFIG = None # Will hold a dictionary of configuration options
    UID_FOR_STATUS_CHANGE = 0;
    def __init__(self,methodName,interfaces):
        super(UserTests,self).__init__(methodName=methodName)
        self.methodName = methodName
        self.interfaces = interfaces

    def setUp(self):
        super(UserTests, self).setUp()
        #TODO: make sure this logs in with the Mr. Manager account
        self.login_admin()

    def setUpToken(self,email):
        userDb = UserHandler()
        token = sesEmail.createToken(email, userDb, "validate_email")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_email_token/", postJson)

    def test_registration_no_token(self):
        self.logout()
        postJson = {"email": "user@agency.gov", "name": "user", "agency": "agency", "title": "title", "password": "userPass"}
        response = self.app.post_json("/v1/check_status/", postJson)
        self.check_response(response, StatusCode.LOGIN_REQUIRED)

    def test_registration(self):
        self.logout()
        email = UserTests.CONFIG["change_user_email"]
        self.setUpToken(email)
        postJson = {"email": email, "name": "user", "agency": "agency", "title": "title", "password": "userPass"}
        response = self.app.post_json("/v1/register/", postJson)
        self.check_response(response, StatusCode.OK, "Registration successful")

    def test_registration_empty(self):
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {}
        response = self.app.post_json("/v1/register/", postJson)
        self.check_response(response, StatusCode.CLIENT_ERROR,
            "Request body must include email, name, agency, title, and password")

    def test_registration_bad_email(self):
        self.logout()
        self.setUpToken("user@agency.gov")
        postJson = {"email": "fake@notreal.faux", "name": "user", "agency": "agency", "title":"title", "password": "userPass"}
        response = self.app.post_json("/v1/register/", postJson)
        self.check_response(response, StatusCode.CLIENT_ERROR, "No users with that email")

    def test_status_change(self):
        deniedInput = {"uid": "UserTests.UID_FOR_STATUS_CHANGE", "new_status": "denied"}
        approvedInput = {"uid": "UserTests.UID_FOR_STATUS_CHANGE", "new_status": "approved"}
        awaitingInput = {"uid":"UserTests.UID_FOR_STATUS_CHANGE", "new_status": "awaiting_approval"}
        emailConfirmed = {"uid" :"UserTests.UID_FOR_STATUS_CHANGE", "new_status": "email_confirmed"}

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
        self.logout()
        self.login_admin()
        badUserId = {"uid": -100, "new_status": "denied"}
        response = self.app.post_json("/v1/change_status/", badUserId)
        self.check_response(response, StatusCode.CLIENT_ERROR, "No users with that uid")

    def test_status_change_bad_status(self):
        badInput = {"uid": UserTests.UID_FOR_STATUS_CHANGE, "new_status": "badInput"}
        response = self.app.post_json("/v1/change_status/", badInput)
        self.check_response(response, StatusCode.CLIENT_ERROR, "Not a valid user status")

    def test_list_users(self):
        postJson = {"status": "awaiting_approval"}
        response = self.app.post_json("/v1/list_users_with_status/", postJson)
        self.check_response(response, StatusCode.OK)
        users = response.json["users"]
        self.assertEqual(len(users), 4)

    def test_list_users_bad_status(self):
        postJson = {"status": "lost"}
        response = self.app.post_json("/v1/list_users_with_status/", postJson)
        self.check_response(response, StatusCode.CLIENT_ERROR, "Not a valid user status")

    def test_get_users_by_type(self):
        agencyUsers = self.interfaces.userDb.getUsersByType("agency_user")
        emails = []
        for admin in agencyUsers:
            emails.append(admin.email)
        self.assertEqual(len(agencyUsers), 11)
        for email in ["realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", "approved@agency.gov", "nefarious@agency.gov"]:
            self.assertIn(email, emails)

    def test_list_submissions(self):
        self.logout()
        self.login_user("approved@agency.gov", "approvedPass")
        response = self.app.get("/v1/list_submissions/")
        self.check_response(response, StatusCode.OK)
        self.assertIn("submission_id_list", response.json)
        self.assertEqual(len(response.json["submission_id_list"]), 5)
        self.logout()

    def test_list_users_with_status_non_admin(self):
        #TODO: make sure self.login() logs in an approved, non-admin agency user
        self.login()
        postJson = {"status": "awaiting_approval"}
        response = self.app.post_json("/v1/list_users_with_status/", postJson)
        self.check_response(response, StatusCode.LOGIN_REQUIRED, "Wrong User Type")
        self.logout()

    def test_finalize_wrong_user(self):
        self.logout()
        self.login_user("user4", "pass")
        postJson = {"upload_id": UserTests.uploadId}
        response = self.app.post_json("/v1/finalize_job/", postJson)
        self.check_response(response, StatusCode.CLIENT_ERROR, "Cannot finalize a job created by a different user")
        self.logout()

    def test_send_email(self):
        # Always use simulator to test emails!
        postJson = {"email": "success@simulator.amazonses.com"}
        response = self.app.post_json("/v1/confirm_email/", postJson)
        self.check_response(response, StatusCode.OK)

    def test_check_email_token_malformed(self):
        postJson = {"token": "12345678"}
        response = self.app.post_json("/v1/confirm_email_token/", postJson)
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_check_email_token(self):
        userDb = UserHandler()
        #make a token based on a user
        token = sesEmail.createToken("user@agency.gov", userDb, "validate_email")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_email_token/", postJson)
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_password_reset_email(self):
        self.logout()
        email = UserTests.CONFIG["password_reset_email"]
        postJson = {"email": email}
        response = self.app.post_json("/v1/reset_password/", postJson)
        self.check_response(response, StatusCode.OK)

        userDb = UserHandler()
        token = sesEmail.createToken(
            UserTests.CONFIG["password_reset_email"], userDb, "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson)
        self.check_response(response, StatusCode.OK, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

        postJson = {"user_email": email,"password": "pass"}
        response = self.app.post_json("/v1/set_password/", postJson)
        self.check_response(response, StatusCode.OK, "Password successfully changed")
        user = userDb.getUserByEmail(email)
        self.assertTrue(user.password_hash)

    def test_check_password_token(self):
        userDb = UserHandler()
        #make a token based on a user
        token = sesEmail.createToken(
            UserTests.CONFIG["admin_email"], userDb, "password_reset")
        postJson = {"token": token}
        response = self.app.post_json("/v1/confirm_password_token/", postJson)
        self.check_response(response, StatusCode, "success")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_VALID)

    def test_check_bad_password_token(self):
        badToken = {"token": "2345"}
        response = self.app.post_json("/v1/confirm_password_token/", badToken)
        self.check_response(response, StatusCode.OK, "Link already used")
        self.assertEqual(response.json["errorCode"], sesEmail.LINK_ALREADY_USED)

    def test_current_user(self):
        response = self.app.get("/v1/current_user/")
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.json["name"], "Mr. Manager")
        self.assertEqual(response.json["agency"], "Unknown")

    @staticmethod
    def setupUserList():
        """ Clear user and jobs database and add a constant sample set """
        #TODO: use this code to create a pytest fixture
        # Get admin email to send test to
        testConfig = open("test.json","r").read()
        UserTests.CONFIG = json.loads(testConfig)

        userEmails = ["user@agency.gov", "realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", UserTests.CONFIG["admin_email"],"approved@agency.gov", "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation","email_confirmed","awaiting_approval","awaiting_approval","awaiting_approval","approved","approved","denied"]
        userPermissions = [0,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.WEBSITE_ADMIN+AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER]

        # Clear users
        setupUserDB(True)
        setupEmails()
        clearJobs()
        userDb = UserHandler()
        userDb.createUserWithPassword( "user3","123abc",Bcrypt())
        userDb.createUserWithPassword( "user4","pass",Bcrypt())
        try:
            userDb.createUserWithPassword(UserTests.CONFIG["change_user_email"],"pass",Bcrypt())
            userDb.createUserWithPassword(UserTests.CONFIG["password_reset_email"],"pass",Bcrypt())
        except Exception as e:
            print("Please ensure that your test.json file has 'admin_email','change_user_email',and 'password_reset_email'")
            raise e
        jobDb = JobHandler()
        # Add new users and set some statuses
        for index in range(len(userEmails)):
            email = userEmails[index]
            userDb.addUnconfirmedEmail(email)
            user = userDb.getUserByEmail(email)
            userDb.changeStatus(user,userStatus[index])
            userDb.setPermission(user,userPermissions[index])
        # Add submissions to one of the users
        user = userDb.getUserByEmail("approved@agency.gov")
        user.username = "approvedUser"
        userDb.setPassword(user,"approvedPass",Bcrypt())
        admin = userDb.getUserByEmail(UserTests.CONFIG["admin_email"])
        userDb.setPassword(admin,"pass",Bcrypt())
        admin.name = "Mr. Manager"
        admin.agency = "Unknown"
        userDb.session.commit()

        statusChangedUser = userDb.getUserByEmail(UserTests.CONFIG["change_user_email"])
        admin.name = "Mr. Manager"
        UserTests.UID_FOR_STATUS_CHANGE  = str(statusChangedUser.user_id)
        statusChangedUser.name = "Test User"
        statusChangedUser.user_status_id = userDb.getUserStatusId("email_confirmed")
        userDb.session.commit()
        isFirstSub = True
        firstSub = None
        for i in range(0,5):
            sub = Submission(user_id = user.user_id)
            if(isFirstSub):
                firstSub = sub
                isFirstSub = False
            jobDb.session.add(sub)

        jobDb.session.commit()
        # Add job to first submission
        job = JobStatus(submission_id = firstSub.submission_id,status_id = 3,type_id = 1, file_type_id = 1)
        jobDb.session.add(job)
        jobDb.session.commit()
        UserTests.uploadId = job.job_id
