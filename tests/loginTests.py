import unittest
from baseTest import BaseTest
from webtest.app import AppError


class LoginTests(BaseTest):
    """ Test login, logout, and session handling """

    def test_login(self):
        """Test broker login."""
        response = self.login_approved_user()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        self.assertIn('success', response.json["message"].lower())
        # assert response.status_code == 200 (pytest syntax: note to future self)
        json = response.json
        self.assertIn("user_id", json)
        self.assertIn("title", json)
        self.assertIn("name", json)
        self.assertIn("agency", json)
        self.assertIn("permissions", json)

    def test_inactive_login(self):
        """Test broker inactive user login"""
        response = self.login_inactive_user()
        self.assertIn("401 UNAUTHORIZED", response.status)

    def test_logout(self):
        """Test broker logout."""
        response = self.logout()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        self.assertIn('success', response.json["message"].lower())

    def test_session_logout1(self):
        """Test session after broker logout."""
        self.logout()
        response = self.session_route()
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        self.assertEquals(response.json["status"].lower(), "false")

    def test_session_logout2(self):
        """Test session after broker login."""
        self.logout()
        self.login_approved_user()
        response = self.session_route()
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        self.assertEquals(response.json["status"].lower(), "true")

    def test_session_logout3(self):
        """Test session after broker logout/login/logout."""
        self.logout()
        self.login_approved_user()
        self.logout()
        response = self.session_route()
        try:
            self.assertIsInstance(response.json, dict)
        except AttributeError:
            self.fail("Response is missing JSON component")
        self.assertEquals(response.json["status"].lower(), "false")

if __name__ == '__main__':
    unittest.main()
