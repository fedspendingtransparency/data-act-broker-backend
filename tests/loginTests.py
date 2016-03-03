import unittest
from baseTest import BaseTest

class LoginTests(BaseTest):
    """ Test login, logout, and session handling """
    # Test login using config file
    def test_login(self):

        response = self.utils.login()

        # Check status is 200
        assert(response.status_code == 200), "Status code is not 200"
        # Check JSON content type header
        assert("Content-Type" in response.headers), "No content type specified"
        # Test content type is correct
        assert(response.headers["Content-Type"]=="application/json"), "Content type is not json"
        # Make sure json part of response exists and is a dict
        json = response.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"
        # Test content of json
        assert(json["message"] == "Login successful"), "Incorrect content in json string"
        assert("user_id" in json), "Mising User Id"
        assert("title" in  json) , "Missing title"
        assert("name" in json),  "Missing Name"
        assert("agency"  in json), "Missing agency"
        assert("permissions"  in json), "Missing permission"

    # Send login route call
    def session_route(self):
        # Create user json for sample user, eventually load this from config file
        # response does not yet exist
        return self.utils.getRequest( "/v1/session/")

    def test_logout(self):
        logoutResponse = self.utils.logout()
        # Check status is 200
        assert(logoutResponse.status_code == 200), "Error status code"
        # Test header contains content-type
        assert("Content-Type" in logoutResponse.headers), "No content type specified"
        # Test content type is correct
        assert(logoutResponse.headers["Content-Type"]=="application/json"), "Content type is not json"
        # Make sure json part of response exists and is a dict
        json = logoutResponse.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"
        # Test content of json
        assert(json["message"] == "Logout successful"), "Incorrect content in json string"

    def test_session_logout1(self):
        self.utils.logout()
        response = self.session_route()
        json = response.json()
        assert(json["status"] == "False"), "Session is still set"

    def test_session_logout2(self):
        self.utils.logout()
        self.utils.login()
        response = self.session_route()
        json = response.json()
        assert(json["status"] == "True"), "Session is not set"

    def test_session_logout3(self):
        self.utils.logout()
        self.utils.login()
        self.utils.logout()
        response = self.session_route()
        json = response.json()
        assert(json["status"] == "False"), "Session is still set"


if __name__ == '__main__':
    unittest.main()
