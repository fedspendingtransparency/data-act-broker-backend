import unittest
from webtest import TestApp
from dataactbroker.app import createApp

class BaseTest(unittest.TestCase):
    """ Test login, logout, and session handling """

    def setUp(self):
        """Set up broker unit tests."""
        app = createApp()
        app.config['TESTING'] = True
        self.app = TestApp(app)

    def tearDown(self):
        """Tear down broker unit tests."""
        #TODO: delete jobs and submissions from db

    def login(self):
        """Log user into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": "user3", "password": "abc123"}
        return self.app.post_json("/v1/login/", user)

    def login_admin(self):
        """Log an admin user into broker."""
        #TODO: put user data in pytest fixture; put credentials in config file
        user = {"username": "admin_email", "password": "admin_password"}
        response = self.app.post_json("/v1/login/", user)
        return response

    def login_user(self, username, password):
        #TODO: combine with login()
        user = {"username": username, "password": password}
        return self.app.post_json("/v1/login/", user)

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


