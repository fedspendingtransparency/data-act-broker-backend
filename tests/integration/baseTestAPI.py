import unittest
from random import randint

from flask_bcrypt import Bcrypt
from webtest import TestApp

from dataactbroker.app import create_app as create_broker_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import create_user_with_password, get_password_hash
from dataactcore.models.domainModels import CGAC
from dataactcore.models.userModel import User, UserAffiliation
from dataactcore.scripts.databaseSetup import drop_database
from dataactcore.scripts.setupUserDB import setup_user_db
from dataactcore.scripts.setupJobTrackerDB import setup_job_tracker_db
from dataactcore.scripts.setupErrorDB import setup_error_db
from dataactcore.scripts.setupValidationDB import setup_validation_db
from dataactcore.scripts.databaseSetup import create_database, run_migrations
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import dataactcore.config
from dataactbroker.scripts.setupEmails import setup_emails
from dataactvalidator.health_check import create_app as create_validator_app
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from tests.unit.dataactcore.factories.user import UserFactory


class BaseTestAPI(unittest.TestCase):
    """ Test login, logout, and session handling """

    @classmethod
    def setUpClass(cls):
        """Set up resources to be shared within a test class"""
        cls.session_id = ""

        with create_validator_app().app_context():
            # update application's db config options so unittests
            # run against test databases
            suite = cls.__name__.lower()
            config = dataactcore.config.CONFIG_DB
            cls.num = randint(1, 9999)
            config['db_name'] = 'unittest{}_{}_data_broker'.format(cls.num, suite)
            dataactcore.config.CONFIG_DB = config
            create_database(CONFIG_DB['db_name'])
            run_migrations()

            # drop and re-create test user db/tables
            setup_user_db()
            # drop and re-create test job db/tables
            setup_job_tracker_db()
            # drop and re-create test error db/tables
            setup_error_db()
            # drop and re-create test validation db/tables
            setup_validation_db()
            # load e-mail templates
            setup_emails()

            # set up default e-mails for tests
            test_users = {
                'admin_user': 'data.act.tester.1@gmail.com',
                'agency_user': 'data.act.test.2@gmail.com',
                'agency_user_2': 'data.act.test.3@gmail.com',
                'no_permissions_user': 'data.act.tester.4@gmail.com'
            }
            user_password = '!passw0rdUp!'
            admin_password = '@pprovedPassw0rdy'

            # get user info and save as class variables for use by tests
            sess = GlobalDB.db().session
            admin_cgac = CGAC(cgac_code='SYS', agency_name='Admin Agency')
            cls.admin_cgac_code = admin_cgac.cgac_code
            sess.add(admin_cgac)
            sess.commit()

            cgac = CGAC(cgac_code='000', agency_name='Example Agency')

            # set up users for status tests
            def add_user(email, name, username, website_admin=False):
                user = UserFactory(
                    email=email, website_admin=website_admin,
                    name=name, username=username,
                    affiliations=[UserAffiliation(
                        cgac=cgac,
                        permission_type_id=PERMISSION_TYPE_DICT['writer']
                    )]
                )
                user.salt, user.password_hash = get_password_hash(user_password, Bcrypt())
                sess.add(user)

            add_user(test_users['agency_user'], "Test User", "testUser")
            add_user(test_users['agency_user_2'], "Test User 2", "testUser2")

            # add new users
            create_user_with_password(test_users["admin_user"], admin_password, Bcrypt(), website_admin=True)
            create_user_with_password(test_users["no_permissions_user"], user_password, Bcrypt())

            agency_user = sess.query(User).filter(User.email == test_users['agency_user']).one()
            cls.agency_user_id = agency_user.user_id

            sess.commit()

        # set up info needed by the individual test classes
        cls.test_users = test_users
        cls.user_password = user_password
        cls.admin_password = admin_password
        cls.local = CONFIG_BROKER['local']

    def setUp(self):
        """Set up broker unit tests."""
        app = create_broker_app()
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        self.app = TestApp(app)

    @classmethod
    def tearDownClass(cls):
        """Tear down class-level resources."""
        GlobalDB.close()
        drop_database(CONFIG_DB['db_name'])

    def tearDown(self):
        """Tear down broker unit tests."""

    def login_admin_user(self):
        """Log an admin user into broker."""
        # TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": self.test_users['admin_user'], "password": self.admin_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id": self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def login_user(self, username=None):
        """Log an agency user (non-admin) into broker."""
        # TODO: put user data in pytest fixture; put credentials in config file
        if username is None:
            username = self.test_users['agency_user']
        user = {"username": username, "password": self.user_password}
        response = self.app.post_json("/v1/login/", user, headers={"x-session-id": self.session_id})
        self.session_id = response.headers["x-session-id"]
        return response

    def logout(self):
        """Log user out of broker."""
        return self.app.post("/v1/logout/", {}, headers={"x-session-id": self.session_id})

    def session_route(self):
        """Get session."""
        return self.app.get("/v1/session/", headers={"x-session-id": self.session_id})

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
