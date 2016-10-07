import unittest
import time
from collections import namedtuple
from datetime import timedelta
from dateutil.parser import parse
from random import randint
from webtest import TestApp
from dataactbroker.app import createApp
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import createUserWithPassword, getPasswordHash
from dataactcore.models import lookups
from dataactcore.models.userModel import AccountType, User, UserStatus
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import dataactcore.config
from dataactbroker.scripts.setupEmails import setupEmails
from flask_bcrypt import Bcrypt

class BaseTestAPI(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        cls.session_id = ""

        with createApp().app_context():

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

            # set up default e-mails for tests
            test_users = {}
            test_users['admin_email'] = 'data.act.tester.1@gmail.com'
            test_users['change_user_email'] = 'data.act.tester.2@gmail.com'
            test_users['password_reset_email'] = 'data.act.tester.3@gmail.com'
            test_users['inactive_email'] = 'data.act.tester.4@gmail.com'
            test_users['password_lock_email'] = 'data.act.test.5@gmail.com'
            test_users['expired_lock_email'] = 'data.act.test.6@gmail.com'
            test_users['agency_admin_email'] = 'data.act.test.7@gmail.com'

            # this email is for a regular agency_user email that is to be used for
            # testing functionality expected by a normal, base user
            test_users['agency_user'] = 'data.act.test.8@gmail.com'
            test_users['approved_email'] = 'approved@agency.gov'
            test_users['submission_email'] = 'submission_test@agency.gov'
            user_password = '!passw0rdUp!'
            admin_password = '@pprovedPassw0rdy'

            # set up users for status tests
            StatusTestUser = namedtuple('StatusTestUser', ['email', 'user_status', 'permissions', 'user_type'])
            StatusTestUser.__new__.__defaults__ = (None, None, AccountType.AGENCY_USER, None)
            status_test_users = []
            status_test_users.append(StatusTestUser('user@agency.gov', 'awaiting_confirmation', 0))
            status_test_users.append(StatusTestUser('realEmail@agency.gov', 'email_confirmed'))
            status_test_users.append(StatusTestUser('waiting@agency.gov', 'awaiting_approval'))
            status_test_users.append(StatusTestUser('impatient@agency.gov', 'awaiting_approval'))
            status_test_users.append(StatusTestUser('watchingPaintDry@agency.gov', 'awaiting_approval'))
            status_test_users.append(StatusTestUser(test_users['admin_email'], 'approved',
                                              AccountType.WEBSITE_ADMIN + AccountType.AGENCY_USER))
            status_test_users.append(StatusTestUser(test_users['approved_email'], 'approved'))
            status_test_users.append(StatusTestUser('nefarious@agency.gov', 'denied'))

            # add new users
            createUserWithPassword(
                test_users["submission_email"], user_password, Bcrypt())
            createUserWithPassword(
                test_users["change_user_email"], user_password, Bcrypt())
            createUserWithPassword(
                test_users["password_reset_email"], user_password, Bcrypt())
            createUserWithPassword(
                test_users["inactive_email"], user_password, Bcrypt())
            createUserWithPassword(
                test_users["password_lock_email"], user_password, Bcrypt())
            createUserWithPassword(
                test_users['expired_lock_email'], user_password, Bcrypt())
            createUserWithPassword(
                test_users['agency_admin_email'], admin_password, Bcrypt(), permission=4)
            createUserWithPassword(
                test_users['agency_user'], user_password, Bcrypt())

            # get user info and save as class variables for use by tests

            sess = GlobalDB.db().session

            agencyUser = sess.query(User).filter(User.email == test_users['agency_user']).one()
            cls.agency_user_id = agencyUser.user_id

            # set the specified account to be expired
            expiredUser = sess.query(User).filter(User.email == test_users['expired_lock_email']).one()
            today = parse(time.strftime("%c"))
            expiredUser.last_login_date = (today-timedelta(days=120)).strftime("%c")
            sess.add(expiredUser)

            # create users for status testing
            for u in status_test_users:
                user = User(
                    email=u.email,
                    permissions=u.permissions,
                    user_status=sess.query(UserStatus).filter(UserStatus.name == u.user_status).one()
                )
                sess.add(user)

            # set up approved user
            user = sess.query(User).filter(User.email == test_users['approved_email']).one()
            user.username = "approvedUser"
            user.cgac_code = "000"
            user.salt, user.password_hash = getPasswordHash(user_password, Bcrypt())
            sess.add(user)
            cls.approved_user_id = user.user_id

            # set up admin user
            admin = sess.query(User).filter(User.email == test_users['admin_email']).one()
            admin.salt, admin.password_hash = getPasswordHash(admin_password, Bcrypt())
            admin.name = "Mr. Manager"
            admin.cgac_code = "SYS"
            sess.add(admin)

            # set up status changed user
            statusChangedUser = sess.query(User).filter(User.email == test_users['change_user_email']).one()
            statusChangedUser.name = "Test User"
            statusChangedUser.user_status = sess.query(UserStatus).filter(UserStatus.name == 'email_confirmed').one()
            sess.add(statusChangedUser)
            cls.status_change_user_id = statusChangedUser.user_id

            # set up deactivated user
            deactivated_user = sess.query(User).filter(User.email == test_users['inactive_email']).one()
            deactivated_user.last_login_date = time.strftime("%c")
            deactivated_user.is_active = False
            sess.add(deactivated_user)

            sess.commit()

        # get lookup dictionaries
        cls.jobStatusDict = lookups.JOB_STATUS_DICT
        cls.jobTypeDict = lookups.JOB_TYPE_DICT
        cls.fileTypeDict = lookups.FILE_TYPE_DICT
        cls.fileStatusDict = lookups.FILE_STATUS_DICT
        cls.ruleSeverityDict = lookups.RULE_SEVERITY_DICT
        cls.errorTypeDict = lookups.ERROR_TYPE_DICT
        cls.publishStatusDict = lookups.PUBLISH_STATUS_DICT
        cls.userStatusDict = lookups.USER_STATUS_DICT

        # set up info needed by the individual test classes
        cls.test_users = test_users
        cls.user_password = user_password
        cls.admin_password = admin_password
        cls.local = CONFIG_BROKER['local']

    def setUp(self):
        """Set up broker unit tests."""
        app = createApp()
        app.config['TESTING'] = True
        self.app = TestApp(app)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        dropDatabase(CONFIG_DB['db_name'])

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
