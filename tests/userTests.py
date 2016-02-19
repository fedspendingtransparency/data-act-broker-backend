import unittest
from baseTest import BaseTest
from dataactbroker.handlers.userHandler import UserHandler

class UserTests(BaseTest):
    """ Test user registration and updates """
    def __init__(self,methodName,interfaces):
        super(UserTests,self).__init__(methodName=methodName)
        self.interfaces = interfaces

    # Test registration
    def test_registration(self):
        input = '{"email":"user@agency.gov","name":"user","agency":"agency","title":"title"}'
        userDb = UserHandler()
        try:
            userDb.addUnconfirmedEmail("user@agency.gov")
        except ValueError:
            # Raises an exception when user is already present
            pass
        response = self.utils.postRequest("/v1/register/",input)

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
        assert(json["message"] == "Registration successful"), "Incorrect content in json string"

    # Test registration
    def test_empty_registration(self):
        input = '{}'
        userDb = UserHandler()
        response = self.utils.postRequest("/v1/register/",input)

        # Check status is 200
        assert(response.status_code == 400)
        # Check JSON content type header
        assert("Content-Type" in response.headers), "No content type specified"
        # Test content type is correct
        assert(response.headers["Content-Type"]=="application/json"), "Content type is not json"
        # Make sure json part of response exists and is a dict
        json = response.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"
        # Test content of json
        assert(json["message"] == "Request body must include email, name, agency, and title"), "Incorrect content in json string"

    # Test registration with bad email
    def test_bad_email(self):
        input = '{"email":"fake@notreal.faux","name":"user","agency":"agency","title":"title"}'
        userDb = UserHandler()
        response = self.utils.postRequest("/v1/register/",input)

        # Check status is 200
        assert(response.status_code == 400)
        # Check JSON content type header
        assert("Content-Type" in response.headers), "No content type specified"
        # Test content type is correct
        assert(response.headers["Content-Type"]=="application/json"), "Content type is not json"
        # Make sure json part of response exists and is a dict
        json = response.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"
        # Test content of json
        assert(json["message"] == "No users with that email"), "Incorrect content in json string"