import unittest
from webtest import TestApp
from dataactbroker.app import createApp
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactcore.models.userModel import AccountType
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.scripts.clearJobs import clearJobs
from dataactbroker.handlers.userHandler import UserHandler
from flask.ext.bcrypt import Bcrypt

class BaseTest(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        #TODO: refactor into a pytest class fixtures and inject as necessary

        #get test users
        try:
            with open('test.json') as test_users_file:
                test_users = json.load(test_users_file)
            test_users = test_users
        except:
            #if no test.json, provide some default e-mail accounts for tests
            test_users = {}
            test_users['admin_email'] = 'data.act.tester.1@gmail.com'
            test_users['change_user_email'] = 'data.act.tester.2@gmail.com'
            test_users['password_reset_email'] = 'data.act.tester.3@gmail.com'
        if 'approved_email' not in test_users:
            test_users['approved_email'] = 'approved@agency.gov'
        if 'submission_email' not in test_users:
            test_users['submission_email'] = 'submission_test@agency.gov'
        user_password = 'pass'
        admin_password = 'approvedPass'

        #setup test users
        userEmails = ["user@agency.gov", "realEmail@agency.gov",
            "waiting@agency.gov", "impatient@agency.gov",
            "watchingPaintDry@agency.gov", test_users["admin_email"],
            test_users["approved_email"], "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation",
            "email_confirmed","awaiting_approval",
            "awaiting_approval","awaiting_approval",
            "approved","approved","denied"]
        userPermissions = [0,AccountType.AGENCY_USER,
            AccountType.AGENCY_USER,AccountType.AGENCY_USER,
            AccountType.AGENCY_USER,
            AccountType.WEBSITE_ADMIN+AccountType.AGENCY_USER,
            AccountType.AGENCY_USER,AccountType.AGENCY_USER]

        # line below drops and re-creates user tables
        #TODO: does it make sense to create records here & then delete in tearDown?
        setupUserDB(True)
        # load e-mail templates
        setupEmails()
        # line below deletes everything from job_dependency, job_status, submission
        #TODO: does it make sense to create records here & then delete in tearDown?
        #clearJobs()

        # Add new users
        userDb = UserHandler()
        userDb.createUserWithPassword(
            test_users["submission_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["change_user_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["password_reset_email"], user_password, Bcrypt())

        # Create users for status testing
        #TODO: remove hard-coded surrogate keys
        for index in range(len(userEmails)):
            email = userEmails[index]
            userDb.addUnconfirmedEmail(email)
            user = userDb.getUserByEmail(email)
            userDb.changeStatus(user,userStatus[index])
            userDb.setPermission(user,userPermissions[index])

        #set up approved user
        user = userDb.getUserByEmail(test_users['approved_email'])
        user.username = "approvedUser"
        userDb.setPassword(user, user_password ,Bcrypt())
        cls.approved_user_id = user.user_id

        #set up admin user
        admin = userDb.getUserByEmail(test_users['admin_email'])
        userDb.setPassword(admin, admin_password, Bcrypt())
        admin.name = "Mr. Manager"
        admin.agency = "Unknown"
        userDb.session.commit()

        #set up status changed user
        statusChangedUser = userDb.getUserByEmail(test_users["change_user_email"])
        cls.status_change_user_id = statusChangedUser.user_id
        statusChangedUser.name = "Test User"
        statusChangedUser.user_status_id = userDb.getUserStatusId("email_confirmed")
        userDb.session.commit()

        #set up info needed by the individual test classes
        cls.test_users = test_users
        cls.user_password = user_password
        cls.admin_password = admin_password
        cls.interfaces = InterfaceHolder()
        cls.jobTracker = cls.interfaces.jobDb
        cls.errorDatabase = cls.interfaces.errorDb
        cls.userDb = cls.interfaces.userDb

    def setUp(self):
        """Set up broker unit tests."""
        app = createApp()
        app.config['TESTING'] = True
        self.app = TestApp(app)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        #TODO: Do interfaces and other resources need cleaned up?

    def tearDown(self):
        """Tear down broker unit tests."""
        #TODO: delete jobs and submissions from db

    def login_approved_user(self):
        """Log an agency user (non-admin) into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['approved_email'], "password": self.user_password}
        return self.app.post_json("/v1/login/", user)

    def login_admin_user(self):
        """Log an admin user into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['admin_email'], "password": self.admin_password}
        response = self.app.post_json("/v1/login/", user)
        return response

    def logout(self):
        """Log user out of broker."""
        return self.app.post("/v1/logout/", {})

    def session_route(self):
        """Get session."""
        return self.app.get("/v1/session/")

    def check_response(self, response, status, message=None):
        """Perform common tests on API responses."""
        self.assertEqual(response.status_code, status)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        json = response.json
        if message:
            self.assertEqual(message, json["message"])

