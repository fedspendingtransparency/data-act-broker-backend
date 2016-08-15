import json
import unittest
import time
from datetime import timedelta
from dateutil.parser import parse
from random import randint
from webtest import TestApp
from dataactbroker.app import createApp
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactcore.models.userModel import AccountType
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import dataactcore.config
from dataactbroker.scripts.setupEmails import setupEmails
from dataactbroker.handlers.userHandler import UserHandler
from flask.ext.bcrypt import Bcrypt

class BaseTestAPI(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        #TODO: refactor into a pytest class fixtures and inject as necessary
        # Prevent interface being reused from last suite
        BaseInterface.interfaces = None
        # Create an empty session ID
        cls.session_id = ""

        # update application's db config options so unittests
        # run against test databases
        suite = cls.__name__.lower()
        config = dataactcore.config.CONFIG_DB
        cls.num = randint(1, 9999)
        config['db_name'] = 'unittest{}_{}_data_broker'.format(
            cls.num, suite)
        dataactcore.config.CONFIG_DB = config
        createDatabase(CONFIG_DB['db_name'])
        runMigrations()

        # drop and re-create test user db/tables
        setupUserDB()
        # drop and re-create test job db/tables
        setupJobTrackerDB()
        # drop and re-create test error db/tables
        setupErrorDB()
        # drop and re-create test validation db/tables
        setupValidationDB()
        # load e-mail templates
        setupEmails()

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
            test_users['inactive_email'] = 'data.act.tester.4@gmail.com'
            test_users['password_lock_email'] = 'data.act.test.5@gmail.com'
            test_users['expired_lock_email'] = 'data.act.test.6@gmail.com'
            test_users['agency_admin_email'] = 'data.act.test.7@gmail.com'

            # This email is for a regular agency_user email that is to be used for testing functionality
            # expected by a normal, base user
            test_users['agency_user'] = 'data.act.test.8@gmail.com'
        if 'approved_email' not in test_users:
            test_users['approved_email'] = 'approved@agency.gov'
        if 'submission_email' not in test_users:
            test_users['submission_email'] = 'submission_test@agency.gov'
        user_password = '!passw0rdUp!'
        admin_password = '@pprovedPassw0rdy'

        #setup test users
        userEmails = ["user@agency.gov", "realEmail@agency.gov",
            "waiting@agency.gov", "impatient@agency.gov",
            "watchingPaintDry@agency.gov", test_users["admin_email"],
            test_users["approved_email"], "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation",
            "email_confirmed", "awaiting_approval",
            "awaiting_approval", "awaiting_approval",
            "approved", "approved", "denied"]
        userPermissions = [0, AccountType.AGENCY_USER,
            AccountType.AGENCY_USER, AccountType.AGENCY_USER,
            AccountType.AGENCY_USER,
            AccountType.WEBSITE_ADMIN+AccountType.AGENCY_USER,
            AccountType.AGENCY_USER, AccountType.AGENCY_USER]

        # Add new users
        userDb = UserHandler()
        userDb.createUserWithPassword(
            test_users["submission_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["change_user_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["password_reset_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["inactive_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users["password_lock_email"], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users['expired_lock_email'], user_password, Bcrypt())
        userDb.createUserWithPassword(
            test_users['agency_admin_email'], admin_password, Bcrypt(), permission=4)
        userDb.createUserWithPassword(
            test_users['agency_user'], user_password, Bcrypt())

        # Set the Agency for the agency user
        agencyUser = userDb.getUserByEmail(test_users['agency_user'])
        userDb.session.commit()
        cls.agency_user_id = agencyUser.user_id

        # Set the specified account to be expired
        expiredUser = userDb.getUserByEmail(test_users['expired_lock_email'])
        today = parse(time.strftime("%c"))
        expiredUser.last_login_date = (today-timedelta(days=120)).strftime("%c")
        userDb.session.commit()

        # Create users for status testing
        for index in range(len(userEmails)):
            email = userEmails[index]
            userDb.addUnconfirmedEmail(email)
            user = userDb.getUserByEmail(email)
            userDb.changeStatus(user, userStatus[index])
            userDb.setPermission(user, userPermissions[index])

        #set up approved user
        user = userDb.getUserByEmail(test_users['approved_email'])
        user.username = "approvedUser"
        user.cgac_code = "000"
        userDb.setPassword(user, user_password, Bcrypt())
        cls.approved_user_id = user.user_id

        #set up admin user
        admin = userDb.getUserByEmail(test_users['admin_email'])
        userDb.setPassword(admin, admin_password, Bcrypt())
        admin.name = "Mr. Manager"
        admin.cgac_code = "SYS"
        userDb.session.commit()

        #set up status changed user
        statusChangedUser = userDb.getUserByEmail(
            test_users["change_user_email"])
        cls.status_change_user_id = statusChangedUser.user_id
        statusChangedUser.name = "Test User"
        statusChangedUser.user_status_id = userDb.getUserStatusId(
            "email_confirmed")
        userDb.session.commit()

        #set up deactivated user
        user = userDb.getUserByEmail(test_users["inactive_email"])
        user.last_login_date = time.strftime("%c")
        user.is_active = False
        userDb.session.commit()

        #set up info needed by the individual test classes
        cls.test_users = test_users
        cls.user_password = user_password
        cls.admin_password = admin_password
        cls.interfaces = InterfaceHolder()
        cls.jobTracker = cls.interfaces.jobDb
        cls.errorDatabase = cls.interfaces.errorDb
        cls.userDb = cls.interfaces.userDb
        cls.validationDb = cls.interfaces.validationDb
        cls.local = CONFIG_BROKER['local']

    def setUp(self):
        """Set up broker unit tests."""
        # Repopulate interfaces if needed
        self.interfaces = InterfaceHolder()
        app = createApp()
        app.config['TESTING'] = True
        self.app = TestApp(app)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        cls.interfaces.close()
        dropDatabase(cls.interfaces.jobDb.dbName)

    def tearDown(self):
        """Tear down broker unit tests."""

    def login_approved_user(self):
        """Log an agency user (non-admin) into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['approved_email'],
            "password": self.user_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id":self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def login_agency_user(self):
        """Log an agency user (non-admin) into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['agency_user'],
            "password": self.user_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id":self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def login_admin_user(self):
        """Log an admin user into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['admin_email'],
            "password": self.admin_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id":self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def login_agency_admin_user(self):
        """ Log an agency admin user into broker. """
        # TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['agency_admin_email'],
                "password": self.admin_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id": self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def login_inactive_user(self):
        """Attempt to log in an inactive user"""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['inactive_email'],
            "password": self.user_password}
        response = self.app.post_json("/v1/login/", user, expect_errors=True, headers={"x-session-id":self.session_id})
        try:
            self.session_id = response.headers["x-session-id"]
        except KeyError:
            # Session ID doesn't come back for inactive user, set to empty
            self.session_id = ""
        return response

    def login_expired_locked_user(self):
        """Force user to have their account locked then attempt to login again"""
        # TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['expired_lock_email'], "password": self.user_password}
        response = self.app.post_json("/v1/login/", user, expect_errors=True, headers={"x-session-id": self.session_id})

        try:
            self.session_id = response.headers["x-session-id"]
        except KeyError:
            # Session ID doesn't come back for inactive user, set to empty
            self.session_id = ""
        return response

    def login_password_locked_user(self):
        """Force user to have their account locked then attempt to login again"""
        # TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['password_lock_email'], "password": "wrongpassword"}
        response = self.app.post_json("/v1/login/", user, expect_errors=True, headers={"x-session-id": self.session_id})

        try:
            self.session_id = response.headers["x-session-id"]
        except KeyError:
            # Session ID doesn't come back for inactive user, set to empty
            self.session_id = ""
        return response

    def login_other_user(self, username, password):
        """Log a specific user into broker."""
        user = {"username": username, "password": password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id":self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def logout(self):
        """Log user out of broker."""
        return self.app.post("/v1/logout/", {}, headers={"x-session-id":self.session_id})

    def session_route(self):
        """Get session."""
        return self.app.get("/v1/session/", headers={"x-session-id":self.session_id})

    def check_response(self, response, status, message=None):
        """Perform common tests on API responses."""
        self.assertEqual(response.status_code, status)
        self.assertEqual(response.headers.get("Content-Type"),
            "application/json")
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        json = response.json
        if message:
            self.assertEqual(message, json["message"])
