import unittest
import requests
import json


class TestUtils:
    # Test basic routes, including login and file submission
    BASE_URL = "http://127.0.0.1:5000"
    JSON_HEADER = {"Content-Type": "application/json"}

    def getRequest(self,url) :
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        responseData = requests.request(method="GET", url=TestUtils.BASE_URL + url, headers = TestUtils.JSON_HEADER,cookies=self.cookies)
        self.cookies =  responseData.cookies
        return responseData

    def postRequest(self,url,jsonData) :
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        responseData = requests.request(method="POST", url=TestUtils.BASE_URL + url, data=jsonData, headers = TestUtils.JSON_HEADER,cookies=self.cookies)
        self.cookies =  responseData.cookies
        return responseData

    # Send login route call
    def login(self):
        userJson = '{"username":"user3","password":"123abc"}'
        return self.postRequest("/v1/login/",userJson)
    # Call logout route
    def logout(self):
        return self.postRequest("/v1/logout/",{})
