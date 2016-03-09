import json
from baseTest import BaseTest
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.clearJobs import clearJobs
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.userModel import AccountType
from dataactcore.utils.statusCode import StatusCode
from flask.ext.bcrypt import Bcrypt
class UserTests(BaseTest):
    """ Test user registration and user specific functions """
    uploadId = None # set in setup function, used for testing wrong user on finalize
    CONFIG = None # Will hold a dictionary of configuration options
    UID_FOR_STATUS_CHANGE = 0;
    def __init__(self,methodName,interfaces):
        super(UserTests,self).__init__(methodName=methodName)
        self.methodName = methodName
        self.interfaces = interfaces
        self.passed = False # Set to true if unit test passes

    def setUp(self):
        self.utils.login(UserTests.CONFIG["admin_email"],"pass") # Log the user in for each test

    def setUpToken(self,email):
        userDb = UserHandler()
        token = sesEmail.createToken(email,userDb,"validate_email")
        json = '{"token":"'+token+'"}'
        self.utils.postRequest("/v1/confirm_email_token/",json)

    def test_registration_no_token(self):
        self.utils.logout()
        input = '{"email":"user@agency.gov","name":"user","agency":"agency","title":"title","password":"userPass"}'
        self.response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(self.response,StatusCode.LOGIN_REQUIRED)
        self.passed = True

    def test_registration(self):
        self.utils.logout()
        email = UserTests.CONFIG["change_user_email"]
        self.setUpToken(email)

        input = '{"email":"'+email+'","name":"user","agency":"agency","title":"title","password":"user1Pass!"}'
        self.response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(self.response,StatusCode.OK,"Registration successful")
        self.passed = True

    def test_registration_empty(self):
        self.utils.logout()
        input = '{}'
        self.setUpToken("user@agency.gov")
        self.response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR,"Request body must include email, name, agency, title, and password")
        self.passed = True

    def test_registration_bad_email(self):
        self.utils.logout()
        self.setUpToken("user@agency.gov")
        input = '{"email":"fake@notreal.faux","name":"user","agency":"agency","title":"title","password":"user1Pass!"}'
        self.response = self.utils.postRequest("/v1/register/",input)
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR,"No users with that email")
        self.passed = True

    def test_status_change(self):
        deniedInput = '{"uid":"'+UserTests.UID_FOR_STATUS_CHANGE+'","new_status":"denied"}'
        approvedInput = '{"uid":"'+UserTests.UID_FOR_STATUS_CHANGE+'","new_status":"approved"}'
        awaitingInput = '{"uid":"'+UserTests.UID_FOR_STATUS_CHANGE+'","new_status":"awaiting_approval"}'
        emailConfirmed = '{"uid":"'+UserTests.UID_FOR_STATUS_CHANGE+'","new_status":"email_confirmed"}'

        self.response = self.utils.postRequest("/v1/change_status/",awaitingInput)
        self.utils.checkResponse(self.response,StatusCode.OK,"Status change successful")


        self.response = self.utils.postRequest("/v1/change_status/",approvedInput)
        self.utils.checkResponse(self.response,StatusCode.OK,"Status change successful")


        self.response = self.utils.postRequest("/v1/change_status/",awaitingInput)
        self.utils.checkResponse(self.response,StatusCode.OK,"Status change successful")


        self.response = self.utils.postRequest("/v1/change_status/",deniedInput)
        self.utils.checkResponse(self.response,StatusCode.OK,"Status change successful")

        # Set back to email_confirmed for register test
        self.response = self.utils.postRequest("/v1/change_status/",emailConfirmed)
        self.utils.checkResponse(self.response,StatusCode.OK,"Status change successful")
        self.passed = True


    def test_status_change_bad_uid(self):
        self.utils.logout()
        self.utils.login(UserTests.CONFIG["admin_email"],"pass")
        badUserId = '{"uid":-100,"new_status":"denied"}'
        self.response = self.utils.postRequest("/v1/change_status/",badUserId)
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR,"No users with that uid")
        self.passed = True

    def test_status_change_bad_status(self):
        badInput = '{"uid":"'+UserTests.UID_FOR_STATUS_CHANGE+'","new_status":"badInput"}'
        self.response = self.utils.postRequest("/v1/change_status/",badInput)
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR)
        self.passed = True

    def test_list_users(self):
        input = '{"status":"awaiting_approval"}'
        self.response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(self.response,StatusCode.OK)
        users = self.response.json()["users"]

        assert(len(users) == 4), "There should be four users awaiting approval"
        self.passed = True

    def test_list_users_bad_status(self):
        input = '{"status":"lost"}'
        self.response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR)
        self.passed = True

    def test_get_users_by_type(self):
        agencyUsers = self.interfaces.userDb.getUsersByType("agency_user")

        emails = []
        for admin in agencyUsers:
            emails.append(admin.email)
        assert(len(agencyUsers) == 11), "There should be ten agency users"
        for email in ["realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", "approved@agency.gov", "nefarious@agency.gov"]:
            assert(email in emails)
        self.passed = True

    def test_list_submissions(self):
        self.utils.logout()
        self.utils.login("approved@agency.gov","approvedPass")
        self.response = self.utils.postRequest("/v1/list_submissions/",{},method="GET")
        self.utils.logout()
        self.utils.checkResponse(self.response,StatusCode.OK)
        responseDict = self.response.json()
        assert("submission_id_list" in responseDict)
        assert(len(responseDict["submission_id_list"]) == 5)
        self.passed = True

    def test_list_users_with_status_non_admin(self):
        self.utils.login("user3","123abc")
        input = '{"status":"awaiting_approval"}'
        self.response = self.utils.postRequest("/v1/list_users_with_status/",input)
        self.utils.logout()
        self.utils.checkResponse(self.response,StatusCode.LOGIN_REQUIRED)
        responseDict = self.response.json()
        assert((responseDict["message"]) == 'Wrong User Type')
        self.passed = True

    def test_finalize_wrong_user(self):
        self.utils.logout()
        self.utils.login("user4","pass")
        self.response = self.utils.postRequest("/v1/finalize_job/",json.dumps({"upload_id":UserTests.uploadId}))
        self.utils.logout()
        self.utils.checkResponse(self.response,StatusCode.CLIENT_ERROR,"Cannot finalize a job created by a different user")
        self.passed = True

    def test_send_email(self):
        # Always use simulator to test emails!
        json = '{"email":"success@simulator.amazonses.com"}'
        self.response = self.utils.postRequest("/v1/confirm_email/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        self.passed = True

    def test_check_email_token_malformed(self):
        json = '{"token":"12345678"}'
        self.response = self.utils.postRequest("/v1/confirm_email_token/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "Link already used")
        assert(self.response.json()["errorCode"]== sesEmail.LINK_ALREADY_USED)
        self.passed = True

    def test_check_email_token(self):
        userDb = UserHandler()
        #make a token based on a user

        token = sesEmail.createToken("user@agency.gov",userDb,"validate_email")
        json = '{"token":"'+token+'"}'
        self.response = self.utils.postRequest("/v1/confirm_email_token/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "success")
        assert(self.response.json()["errorCode"]== sesEmail.LINK_VALID)
        self.passed = True

    def test_password_reset_email(self):
        self.utils.logout()
        email = UserTests.CONFIG["password_reset_email"]
        json = '{"email":"'+email+'"}'
        self.response = self.utils.postRequest("/v1/reset_password/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)

        userDb = UserHandler()
        token = sesEmail.createToken(UserTests.CONFIG["password_reset_email"],userDb,"password_reset")
        json = '{"token":"'+token+'"}'
        self.response = self.utils.postRequest("/v1/confirm_password_token/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "success")
        assert(self.response.json()["errorCode"]== sesEmail.LINK_VALID)

        json = '{"user_email":"'+email+'","password":"passPass1!"}'
        self.response = self.utils.postRequest("/v1/set_password/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "Password successfully changed")
        user = userDb.getUserByEmail(email)
        assert(user.password_hash is not None)

        self.passed = True

    def test_check_password_token(self):

        userDb = UserHandler()
        #make a token based on a user
        token = sesEmail.createToken(UserTests.CONFIG["admin_email"],userDb,"password_reset")
        json = '{"token":"'+token+'"}'
        self.response = self.utils.postRequest("/v1/confirm_password_token/",json)
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "success")
        assert(self.response.json()["errorCode"]== sesEmail.LINK_VALID)

        self.passed = True

    def test_check_bad_password_token(self):
        self.response = self.utils.postRequest("/v1/confirm_password_token/",'{"token":"2345"}')
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["message"]== "Link already used")
        assert(self.response.json()["errorCode"]== sesEmail.LINK_ALREADY_USED)
        self.passed = True

    def test_current_user(self):
        self.response = self.utils.getRequest("/v1/current_user/")
        self.utils.checkResponse(self.response,StatusCode.OK)
        assert(self.response.json()["name"]== "Mr. Manager")
        assert(self.response.json()["agency"]== "Unknown")
        self.passed = True

    def tearDown(self):
        if(not self.passed):
            print("".join(["Test failed: ",self.methodName]))
            print("Status is " + str(self.response.status_code))
            print(str(self.response.json()))

    @staticmethod
    def setupUserList():
        """ Clear user and jobs database and add a constant sample set """
        # Get admin email to send test to
        testConfig = open("test.json","r").read()
        UserTests.CONFIG = json.loads(testConfig)

        userEmails = ["user@agency.gov", "realEmail@agency.gov", "waiting@agency.gov", "impatient@agency.gov", "watchingPaintDry@agency.gov", UserTests.CONFIG["admin_email"],"approved@agency.gov", "nefarious@agency.gov"]
        userStatus = ["awaiting_confirmation","email_confirmed","awaiting_approval","awaiting_approval","awaiting_approval","approved","approved","denied"]
        userPermissions = [0,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.WEBSITE_ADMIN+AccountType.AGENCY_USER,AccountType.AGENCY_USER,AccountType.AGENCY_USER]

        # Clear users
        setupUserDB(True)
        setupEmails()
        clearJobs()
        userDb = UserHandler()
        userDb.createUserWithPassword( "user3","123abc",Bcrypt())
        userDb.createUserWithPassword( "user4","pass",Bcrypt())
        try:
            userDb.createUserWithPassword(UserTests.CONFIG["change_user_email"],"pass",Bcrypt())
            userDb.createUserWithPassword(UserTests.CONFIG["password_reset_email"],"pass",Bcrypt())
        except Exception as e:
            print("Please ensure that your test.json file has 'admin_email','change_user_email',and 'password_reset_email'")
            raise e
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
        userDb.setPassword(user,"approvedPass",Bcrypt())
        admin = userDb.getUserByEmail(UserTests.CONFIG["admin_email"])
        userDb.setPassword(admin,"pass",Bcrypt())
        admin.name = "Mr. Manager"
        admin.agency = "Unknown"
        userDb.session.commit()

        statusChangedUser = userDb.getUserByEmail(UserTests.CONFIG["change_user_email"])
        admin.name = "Mr. Manager"
        UserTests.UID_FOR_STATUS_CHANGE  = str(statusChangedUser.user_id)
        statusChangedUser.name = "Test User"
        statusChangedUser.user_status_id = userDb.getUserStatusId("email_confirmed")
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
