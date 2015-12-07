import unittest
import requests
import json


class TestUtils:
    # Test basic routes, including login and file submission
    BASE_URL = "http://127.0.0.1:5000"
    JSON_HEADER = {"Content-Type": "application/json"}

    # Send login route call
    def login(self):
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        userJson = '{"username":"user3","password":"123abc"}'
        self.response = requests.request(method="POST", url=TestUtils.BASE_URL + "/v1/login/", data = userJson, headers = TestUtils.JSON_HEADER,cookies=self.cookies)
        self.cookies =  self.response.cookies
        return self.response
    # Call logout route
    def logout(self):
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        self.logoutResponse = requests.request(method="POST", url=TestUtils.BASE_URL + "/v1/logout/", headers = TestUtils.JSON_HEADER,cookies=self.cookies)
        self.cookies =  self.logoutResponse.cookies
        return self.logoutResponse


