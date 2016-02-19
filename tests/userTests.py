import unittest
from baseTest import BaseTest
from dataactbroker.handlers.userHandler import UserHandler
from dataactcore.scripts.setupUserDB import setupUserDB

class UserTests(BaseTest):
    """ Test user registration and updates """
    def __init__(self,methodName,interfaces):
        super(UserTests,self).__init__(methodName=methodName)
        self.interfaces = interfaces

    def test_registration(self):
        input = '{"email":"user@agency.gov","name":"user","agency":"agency","title":"title"}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,200,"Registration successful")

    def test_registration_empty(self):
        input = '{}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,400,"Request body must include email, name, agency, and title")

    def test_registration_bad_email(self):
        input = '{"email":"fake@notreal.faux","name":"user","agency":"agency","title":"title"}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,400,"No users with that email")

    def test_status_change(self):
        input = '{"user_email":"user@agency.gov","new_status":"denied"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,200,"Status change successful")

    def test_status_change_bad_email(self):
        input = '{"user_email":"fake@notreal.faux","new_status":"denied"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,400,"No users with that email")

    def test_status_change_bad_status(self):
        input = '{"user_email":"user@agency.gov","new_status":"disoriented"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,400,"Not a valid user status")

    def test_list_users(self):
        input = '{"status":"awaiting_approval"}'
        response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(response,200)
        users = response.json()["users"]
        assert(len(users) == 3), "There should be three users awaiting approval"

    def test_list_users_bad_status(self):
        input = '{"status":"lost"}'
        response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(response,400,"Not a valid user status")

    def test_get_users_by_type(self):

    @staticmethod
    def setupUserList():
        """ Clear user database and add a constant sample set """
        userEmails = ["user@agency.gov", "realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", "approved@agency.gov", "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation","email_confirmed","awaiting_approval","awaiting_approval","awaiting_approval","approved","denied"]
        userPermissions = [0,2,1,1,1,2,2]
        # Clear users
        setupUserDB(True)
        userDb = UserHandler()
        # Add new users and set some statuses
        for index in range(len(userEmails)):
            email = userEmails[index]
            userDb.addUnconfirmedEmail(email)
            user = userDb.getUserByEmail(email)
            userDb.changeStatus(user,userStatus[index])
            userDb.setPermission(user,userPermissions[index])