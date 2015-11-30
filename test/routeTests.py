import unittest
import requests
import json

class RouteTests(unittest.TestCase):
    # Test basic routes, including login and file submission
    baseUrl = "http://127.0.0.1:5000"

    # Test login using config file
    def test_login_status(self):
        self.login()

        # Check status is 200
        assert(self.response.status_code == 200), "Error status code"

    # Test header contains content-type
    def test_header_has_type(self):
        self.login()
        # Check JSON content type header
        #print(str(self.response.headers))
        assert("Content-Type" in self.response.headers), "No content type specified"

    # Test content type is correct
    def test_type_is_json(self):
        self.login()
        assert(self.response.headers["Content-Type"]=="application/json"), "Content type is not json"

    # Make sure json part of response exists and is a dict
    def test_json_exists(self):
        self.login()
        json = self.response.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"

    # Test content of json
    def test_json_content(self):
        self.login()
        json = self.response.json()
        assert(json["message"] == "Login successful"), "Incorrect content in json string"

    # Send login route call
    def login(self):
        # Create user json for sample user, eventually load this from config file
        # Check for current login response
        try:
            self.response
        except (NameError, AttributeError) as e:
            # response does not yet exist
            userJson = '{"username":"user3","password":"123abc"}'
            headerDict = {"Content-Type": "application/json"}
            self.response = requests.request(method="POST", url=self.baseUrl + "/v1/login/", data = userJson, headers = headerDict)

    # Call logout route
    def logout(self):
        try:
            self.logoutResponse
        except (NameError, AttributeError) as e:
            # response does not yet exist
            headerDict = {"Content-Type": "application/json"}
            self.logoutResponse = requests.request(method="GET", url=self.baseUrl + "/v1/logout/", headers = headerDict)

    def test_logout_status(self):
        self.logout()
        # Check status is 200
        assert(self.logoutResponse.status_code == 200), "Error status code"

    # Test header contains content-type
    def test_logout_header_has_type(self):
        self.logout()
        # Check JSON content type header
        #print(str(self.response.headers))
        assert("Content-Type" in self.logoutResponse.headers), "No content type specified"

    # Test content type is correct
    def test_logout_type_is_json(self):
        self.logout()
        assert(self.logoutResponse.headers["Content-Type"]=="application/json"), "Content type is not json"

    # Make sure json part of response exists and is a dict
    def test_logout_json_exists(self):
        self.logout()
        json = self.logoutResponse.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"

    # Test content of json
    def test_logout_json_content(self):
        self.logout()
        json = self.logoutResponse.json()
        assert(json["message"] == "Logout successful"), "Incorrect content in json string"

    # # Tests for session handling
    # def test_session_start(self):
    #     print("Testing session start")
    #     response = requests.get(self.baseUrl + "/v1/create_session/")
    #     print(response)
    #
    #     responseDict = json.loads(response.content)
    #     # message must exist and be correct
    #     assert("message" in responseDict)
    #     assert(responseDict["message"] =="Session created")
    #     # session key must exist and represent a positive integer
    #     assert("session_key" in responseDict)
    #     key = 0
    #     try:
    #         key = int(responseDict["session_key"])
    #         isInt = True
    #     except Exception:
    #         isInt = False
    #     assert(isInt)
    #     assert(key > 0)
    #     return True
    #
    # def test_session_key_lookup(self):
    #     print("Testing session key lookup")
    #     response = requests.get(self.baseUrl + "/v1/session_key/")
    #     # What should this return?
    #     # Fail this test until desired behavior is defined
    #     assert(False)
    #
    # # Tests for file submission
    # def test_file_submission(self):
    #     # Behavior not defined
    #     assert(False)
    #
    # # Tests for validation
    # def test_validation(self):
    #     # Behavior not defined
    #     assert(False)

if __name__ == '__main__':
    unittest.main()