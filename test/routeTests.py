import unittest
import requests
import json
class RouteTests(unittest.TestCase):
    # Test basic routes, including login and file submission
    BASE_URL = "http://127.0.0.1:5000"
    JSON_HEADER = {"Content-Type": "application/json"}
    fileResponse = None
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
    def session_route(self):
        # Create user json for sample user, eventually load this from config file
        # response does not yet exist
        headerDict = {"Content-Type": "application/json"}
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        self.response = requests.request(method="GET", url=RouteTests.BASE_URL + "/v1/session/", headers = RouteTests.JSON_HEADER,cookies=self.cookies)
        self.cookies =  self.response.cookies
    # Send login route call
    def login(self):
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        userJson = '{"username":"user3","password":"123abc"}'
        self.response = requests.request(method="POST", url=RouteTests.BASE_URL + "/v1/login/", data = userJson, headers = RouteTests.JSON_HEADER,cookies=self.cookies)
        self.cookies =  self.response.cookies
    # Call logout route
    def logout(self):
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        self.logoutResponse = requests.request(method="GET", url=RouteTests.BASE_URL + "/v1/logout/", headers = RouteTests.JSON_HEADER,cookies=self.cookies)
        self.cookies =  self.logoutResponse.cookies
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

    def test_session_logout1(self):
        self.logout()
        self.session_route()
        json = self.response.json()
        assert(json["status"] == "False"), "Session is still set"

    def test_session_logout2(self):
        self.logout()
        self.login()
        self.session_route()
        json = self.response.json()
        assert(json["status"] == "True"), "Session is not set"

    def test_session_logout3(self):
        self.logout()
        self.login()
        self.logout()
        self.session_route()
        json = self.response.json()
        assert(json["status"] == "False"), "Session is stil set"

    def call_file_submission(self):
        # If fileResponse doesn't exist, send the request
        userJson = '{"appropriations_url":"test1.csv","award_financial_url":"test2.csv","award_url":"test3.csv","procurement_url":"test4.csv"}'
        if(self.fileResponse == None):
            self.login()
            self.__class__.fileResponse = requests.request(method="POST",data = userJson, url=RouteTests.BASE_URL + "/v1/submit_files/", headers = RouteTests.JSON_HEADER,cookies=self.cookies)

    def test_file_sub_status(self):
        self.call_file_submission()
        assert(self.fileResponse.status_code==200)

    def test_file_sub_content_type_exists(self):
        self.call_file_submission()
        assert("Content-Type" in self.fileResponse.headers)

    def test_file_sub_content_type_json(self):
        self.call_file_submission()
        assert(self.fileResponse.headers["Content-Type"]=="application/json")

    def test_file_sub_message(self):
        self.call_file_submission()

        assert("_test1.csv" in self.fileResponse.json()["appropriations_url"] )
        assert("_test2.csv" in self.fileResponse.json()["award_financial_url"])
        assert("_test3.csv" in self.fileResponse.json()["award_url"])
        assert("_test4.csv" in self.fileResponse.json()["procurement_url"])
        assert("?Signature" in self.fileResponse.json()["appropriations_url"] )
        assert("?Signature" in self.fileResponse.json()["award_financial_url"])
        assert("?Signature" in self.fileResponse.json()["award_url"])
        assert("?Signature" in self.fileResponse.json()["procurement_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["appropriations_url"] )
        assert("&AWSAccessKeyId" in self.fileResponse.json()["award_financial_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["award_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["procurement_url"])

    def test_job_ids_exist(self):
        self.call_file_submission()
        responseDict = self.fileResponse.json()
        idKeys = ["procurement_id", "award_id", "award_financial_id", "appropriations_id"]
        for key in idKeys:
            assert(key in responseDict)
            try:
                int(responseDict[key])
            except:
                self.fail("One of the job ids returned was not an integer")
            # Call upload complete route for each id
        self.check_upload_complete(responseDict["procurement_id"])

    def check_upload_complete(self, jobId):
        userJson = json.dumps({"upload_id":jobId})
        self.login()
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        finalizeResponse = requests.request(method="POST",data = userJson, url=RouteTests.BASE_URL + "/v1/finalize_submission/", headers = RouteTests.JSON_HEADER, cookies = self.cookies)
        self.cookies =  finalizeResponse.cookies
        if(finalizeResponse.status_code != 200):
            print(finalizeResponse.status_code)
            print(finalizeResponse.json()["errorType"])
            print(finalizeResponse.json()["message"])
            print(finalizeResponse.json()["trace"])
        assert(finalizeResponse.status_code == 200)

if __name__ == '__main__':
    unittest.main()
