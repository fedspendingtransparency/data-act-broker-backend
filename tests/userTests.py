import unittest
import json
from baseTest import BaseTest
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.clearJobs import clearJobs
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.userModel import AccountType
from dataactcore.utils.statusCode import StatusCode

class UserTests(BaseTest):
    """ Test user registration and user specific functions """
    uploadId = None # set in setup function, used for testing wrong user on finalize

    def __init__(self,methodName,interfaces):
        super(UserTests,self).__init__(methodName=methodName)
        self.interfaces = interfaces

    def setUp(self):
        self.utils.login() # Log the user in for each test

    def test_registration(self):
        input = '{"email":"user@agency.gov","name":"user","agency":"agency","title":"title","password":"userPass"}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,StatusCode.OK,"Registration successful")

    def test_registration_empty(self):
        input = '{}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"Request body must include email, name, agency, and title")

    def test_registration_bad_email(self):
        input = '{"email":"fake@notreal.faux","name":"user","agency":"agency","title":"title","password":"userPass"}'
        response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"No users with that email")

    def test_status_change(self):
        input = '{"user_email":"user@agency.gov","new_status":"denied"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,StatusCode.OK,"Status change successful")

    def test_status_change_bad_email(self):
        input = '{"user_email":"fake@notreal.faux","new_status":"denied"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"No users with that email")

    def test_status_change_bad_status(self):
        input = '{"user_email":"user@agency.gov","new_status":"disoriented"}'
        response = self.utils.postRequest("/v1/change_status/",input)
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"Not a valid user status")

    def test_list_users(self):
        input = '{"status":"awaiting_approval"}'
        response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(response,StatusCode.OK)
        users = response.json()["users"]
        assert(len(users) == 3), "There should be three users awaiting approval"

    def test_list_users_bad_status(self):
        input = '{"status":"lost"}'
        response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"Not a valid user status")

    def test_get_users_by_type(self):
        admins = self.interfaces.userDb.getUsersByType("agency_user")

        emails = []
        for admin in admins:
            emails.append(admin.email)
        assert(len(admins) == 6), "There should be six agency users"
        for email in ["realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", "approved@agency.gov", "nefarious@agency.gov"]:
            assert(email in emails)

    def test_list_submissions(self):
        self.utils.logout()
        self.utils.login("approvedUser","approvedPass")
        response = self.utils.postRequest("/v1/list_submissions/",{},method="GET")
        self.utils.logout()
        self.utils.checkResponse(response,StatusCode.OK)
        responseDict = response.json()
        assert("submission_id_list" in responseDict)
        assert(len(responseDict["submission_id_list"]) == 5)

    def test_finalize_wrong_user(self):
        self.utils.logout()
        self.utils.login("deniedUser","deniedPass")
        response = self.utils.postRequest("/v1/finalize_job/",json.dumps({"upload_id":UserTests.uploadId}))
        self.utils.logout()
        self.utils.checkResponse(response,StatusCode.CLIENT_ERROR,"Cannot finalize a job created by a different user")

    def test_send_email(self):
        # Always use simulator to test emails!
        json = '{"email":"success@simulator.amazonses.com"}'
        response = self.utils.postRequest("/v1/confirm_email/",json)
        self.utils.checkResponse(response,StatusCode.OK)

    def test_check_email_token_malformed(self):
        json = '{"token":"12345678"}'
        response = self.utils.postRequest("/v1/confirm_email_token/",json)
        self.utils.checkResponse(response,StatusCode.OK)
        assert(response.json()["message"]== "Link already used")

    def test_check_email_token(self):
        userDb = UserHandler()
        #make a token based on a user
        token = sesEmail.createToken("user@agency.gov",userDb,"validate_email")
        json = '{"token":"'+token+'"}'
        response = self.utils.postRequest("/v1/confirm_email_token/",json)
        self.utils.checkResponse(response,StatusCode.OK)
        assert(response.json()["message"]== "success")

    @staticmethod
    def setupUserList():
        """ Clear user and jobs database and add a constant sample set """
        userEmails = ["user@agency.gov", "realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", "approved@agency.gov", "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation","email_confirmed","awaiting_approval","awaiting_approval","awaiting_approval","approved","denied"]
        userPermissions = [0,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.WEBSITE_ADMIN+AccountType.AGENCY_USER,AccountType.AGENCY_USER]
        # Clear users
        setupUserDB(True)
        clearJobs()
        userDb = UserHandler()
        jobDb = JobHandler()
        # Add new users and set some statuses
        for index in range(len(userEmails)):
            email = userEmails[index]
            userDb.addUnconfirmedEmail(email)
            user = userDb.getUserByEmail(email)
            userDb.changeStatus(user,userStatus[index])
            userDb.setPermission(user,userPermissions[index])
        # Add submissions to one of the users
        user = userDb.getUserByEmail("approved@agency.gov")
        user.username = "approvedUser"
        user.name = "Mr. Manager"
        userDb.session.commit()
        isFirstSub = True
        firstSub = None
        for i in range(0,5):
            sub = Submission(user_id = user.user_id)
            if(isFirstSub):
                firstSub = sub
                isFirstSub = False
            jobDb.session.add(sub)

        jobDb.session.commit()
        # Add job to first submission
        job = JobStatus(submission_id = firstSub.submission_id,status_id = 3,type_id = 1, file_type_id = 1)
        jobDb.session.add(job)
        jobDb.session.commit()
        UserTests.uploadId = job.job_id