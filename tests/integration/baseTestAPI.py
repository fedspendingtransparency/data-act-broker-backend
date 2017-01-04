import unittest
from random import randint

from flask_bcrypt import Bcrypt
from webtest import TestApp

from dataactbroker.app import createApp as createBrokerApp
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import createUserWithPassword, getPasswordHash
from dataactcore.models import lookups
from dataactcore.models.domainModels import CGAC
from dataactcore.models.userModel import User, UserAffiliation
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import dataactcore.config
from dataactbroker.scripts.setupEmails import setupEmails
from dataactvalidator.app import createApp as createValidatorApp
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from tests.unit.dataactcore.factories.user import UserFactory


class BaseTestAPI(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        cls.session_id = ""

        with createValidatorApp().app_context():
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
            test_users = {
                'admin_user': 'data.act.tester.1@gmail.com',
                'agency_user': 'data.act.test.2@gmail.com',
                'approved_email': 'approved@agency.gov',
                'no_permissions_user': 'data.act.tester.3@gmail.com'
            }
            user_password = '!passw0rdUp!'
            admin_password = '@pprovedPassw0rdy'

            # get user info and save as class variables for use by tests
            sess = GlobalDB.db().session
            cgac = CGAC(cgac_code='000', agency_name='Example Agency')

            # set up users for status tests
            def add_status_user(email, status_name, website_admin=False):
                sess.add(UserFactory(
                    email=email, website_admin=website_admin,
                    affiliations=[UserAffiliation(
                        cgac=cgac,
                        permission_type_id=PERMISSION_TYPE_DICT['writer']
                    )]
                ))
            add_status_user(test_users['approved_email'], 'approved')

            # add new users
            createUserWithPassword(
                test_users["admin_user"], admin_password, Bcrypt(),
                website_admin=True
            )
            createUserWithPassword(
                test_users["no_permissions_user"], user_password, Bcrypt()
            )

            user = UserFactory(
                email=test_users['agency_user'], website_admin=False,
                affiliations=[UserAffiliation(
                    cgac=cgac,
                    permission_type_id=PERMISSION_TYPE_DICT['writer']
                )]
            )
            user.salt, user.password_hash = getPasswordHash(user_password, Bcrypt())
            user.name = "Test User"
            user.username = "testUser"
            sess.add(user)

            agencyUser = sess.query(User).filter(User.email == test_users['agency_user']).one()
            cls.agency_user_id = agencyUser.user_id

            # set up approved user
            user = sess.query(User).filter(User.email == test_users['approved_email']).one()
            user.username = "approvedUser"
            user.cgac_code = "000"
            user.salt, user.password_hash = getPasswordHash(user_password, Bcrypt())
            sess.add(user)
            cls.approved_user_id = user.user_id

            sess.commit()

        # get lookup dictionaries
        cls.jobStatusDict = lookups.JOB_STATUS_DICT
        cls.jobTypeDict = lookups.JOB_TYPE_DICT
        cls.fileTypeDict = lookups.FILE_TYPE_DICT
        cls.fileStatusDict = lookups.FILE_STATUS_DICT
        cls.ruleSeverityDict = lookups.RULE_SEVERITY_DICT
        cls.errorTypeDict = lookups.ERROR_TYPE_DICT
        cls.publishStatusDict = lookups.PUBLISH_STATUS_DICT

        # set up info needed by the individual test classes
        cls.test_users = test_users
        cls.user_password = user_password
        cls.admin_password = admin_password
        cls.local = CONFIG_BROKER['local']

    def setUp(self):
        """Set up broker unit tests."""
        app = createBrokerApp()
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        self.app = TestApp(app)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        GlobalDB.close()
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

    def login_admin_user(self):
        """Log an admin user into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['admin_user'],
            "password": self.admin_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id":self.session_id})
        self.session_id = response.headers["x-session-id"]
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
