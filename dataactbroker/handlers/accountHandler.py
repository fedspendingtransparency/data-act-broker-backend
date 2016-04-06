from flask import session as flaskSession
from threading import Thread
import re
import time
from dateutil.parser import parse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession

class AccountHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    FRONT_END = ""
    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request, interfaces = None, bcrypt = None):
        """

        Creates the Login Handler
        """
        self.request = request
        self.bcrypt = bcrypt
        if(interfaces != None):
            self.interfaces = interfaces
            self.userManager = interfaces.userDb

    def addInterfaces(self,interfaces):
        """ Add interfaces to an existing account handler """
        self.interfaces = interfaces
        self.userManager = interfaces.userDb

    def checkPassword(self,password):
        """Checks to make sure the password is valid"""
        if( re.search(r"[\[\]\{\}~!@#$%^,.?;<>]", password) is None or len(password) < 8 or re.search(r"\d", password) is None or
            re.search(r"[A-Z]", password) is None or re.search(r"[a-z]", password) is None) :
            return False
        return True

    def login(self,session):
        """

        Logs a user in if their password matches

        arguments:

        session  -- (Session) object from flask

        return the reponse object

        """
        try:
            safeDictionary = RequestDictionary(self.request)

            username = safeDictionary.getValue('username')

            password = safeDictionary.getValue('password')

            try:
                user  = self.interfaces.userDb.getUserByEmail(username)
            except Exception as e:
                raise ValueError("user name and or password invalid")

            if(not self.interfaces.userDb.checkStatus(user,"approved")):
                raise ValueError("user name and or password invalid")

            # Only check if user is active after they've logged in for the first time
            if user.last_login_date is not None and not self.isUserActive(user):
                raise ValueError("user name and or password invalid")

            try:
                if(self.interfaces.userDb.checkPassword(user,password,self.bcrypt)):
                    # We have a valid login
                    LoginSession.login(session,user.user_id)
                    permissionList = []
                    for permission in self.interfaces.userDb.getPermssionList():
                        if(self.interfaces.userDb.hasPermisson(user,permission.name)):
                            permissionList.append(permission.permission_type_id)
                    self.interfaces.userDb.updateLastLogin(user)
                    return JsonResponse.create(StatusCode.OK,{"message":"Login successful","user_id": int(user.user_id),"name":user.name,"title":user.title ,"agency":user.agency, "permissions" : permissionList})
                else :
                    raise ValueError("user name and or password invalid")
            except Exception as e:
                    LoginSession.logout(session)
                    raise ValueError("user name and or password invalid")

        except (TypeError, KeyError, NotImplementedError) as e:
            # Return a 400 with appropriate message
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ValueError as e:
            # Return a 401 for login denied
            return JsonResponse.error(e,StatusCode.LOGIN_REQUIRED)
        except Exception as e:
            # Return 500
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
        return self.response


    def logout(self,session):
        """

        This function removes the session from the session table if currently logged in, and then returns a success message

        arguments:

        session  -- (Session) object from flask

        return the reponse object

        """
        # Call session handler
        LoginSession.logout(session)
        return JsonResponse.create(StatusCode.OK,{"message":"Logout successful"})


    def register(self,system_email,session):
        """

        Save user's information into user database.  Associated request body should have keys 'email', 'name', 'agency', and 'title'

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask


        Returns message that registration is successful or error message that fields are not valid

        """
        def ThreadedFunction (from_email="",username="",title="",agency="",userEmail="" ,link="") :
            """
            This inner function sends emails in a new thread as there could be lots of admins

            from_email -- (string) the from email address
            username -- (string) the name of the  user
            title  --   (string) the title of the  user
            agency -- (string) the agency of the  user
            userEmail -- (string) the email of the user
            link  -- (string) the broker email link
            """
            threadedDatabase =  UserHandler()
            try:
                for user in threadedDatabase.getUsersByType("website_admin") :
                    emailTemplate = {'[REG_NAME]': username, '[REG_TITLE]':title, '[REG_AGENCY]':agency,'[REG_EMAIL]' : userEmail,'[URL]':link}
                    newEmail = sesEmail(user.email, system_email,templateType="account_creation",parameters=emailTemplate,database=threadedDatabase)
                    newEmail.send()
            finally:
                InterfaceHolder.closeOne(threadedDatabase)

        requestFields = RequestDictionary(self.request)
        if(not (requestFields.exists("email") and requestFields.exists("name") and requestFields.exists("agency") and requestFields.exists("title") and requestFields.exists("password"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include email, name, agency, title, and password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if(not self.checkPassword(requestFields.getValue("password"))):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        # Add user info to database
        self.interfaces.userDb.addUserInfo(user,requestFields.getValue("name"),requestFields.getValue("agency"),requestFields.getValue("title"))
        self.interfaces.userDb.setPassword(user,requestFields.getValue("password"),self.bcrypt)

        userLink= AccountHandler.FRONT_END
        # Send email to approver list
        emailThread = Thread(target=ThreadedFunction, kwargs=dict(from_email=system_email,username=user.name,title=user.title,agency=user.agency,userEmail=user.email,link=userLink))
        emailThread.start()

        #email user
        link= AccountHandler.FRONT_END
        emailTemplate = {'[EMAIL]' : system_email}
        newEmail = sesEmail(user.email, system_email,templateType="account_creation_user",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()

        LoginSession.logout(session)
        # Mark user as awaiting approval
        self.interfaces.userDb.changeStatus(user,"awaiting_approval")
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def createEmailConfirmation(self,system_email,session):
        """

        Creates user record and email

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask

        """
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("email")):
            exc = ResponseException("Request body must include email", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        email = requestFields.getValue("email")
        if( not re.match("[^@]+@[^@]+\.[^@]+",email)) :
            return JsonResponse.error(ValueError("Invalid Email Format"),StatusCode.CLIENT_ERROR)
        try :
            user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        except ResponseException as e:
            self.interfaces.userDb.addUnconfirmedEmail(email)
        else:
            if(not (user.user_status_id == self.interfaces.userDb.getUserStatusId("awaiting_confirmation") or user.user_status_id == self.interfaces.userDb.getUserStatusId("email_confirmed"))):
                exc = ResponseException("User already registered", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status)
        emailToken = sesEmail.createToken(email,self.interfaces.userDb,"validate_email")
        link= "".join([AccountHandler.FRONT_END,'/registration/',emailToken])
        emailTemplate = {'[USER]': email, '[URL]':link}
        newEmail = sesEmail(email, system_email,templateType="validate_email",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()
        return JsonResponse.create(StatusCode.OK,{"message":"Email Sent"})

    def checkEmailConfirmationToken(self,session):
        """

        Creates user record and email

        arguments:

        session -- (Session) object from flask

        return the reponse object with a error code and a message

        """
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        token = requestFields.getValue("token")
        success,message,errorCode = sesEmail.checkToken(token,self.interfaces.userDb,"validate_email")
        if(success):
            #mark session that email can be filled out
            LoginSession.register(session)
            #remove token so it cant be used again
            self.interfaces.userDb.deleteToken(token)
            #set the status
            self.interfaces.userDb.changeStatus(self.interfaces.userDb.getUserByEmail(message),"email_confirmed")
            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})

    def checkPasswordToken(self,session):
        """

        Checks the password token if its valid

        arguments:

        session -- (Session) object from flask

        return the reponse object with a error code and a message

        """
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        token = requestFields.getValue("token")
        success,message,errorCode = sesEmail.checkToken(token,self.interfaces.userDb,"password_reset")
        if(success):
            #mark session that password can be filled out
            LoginSession.resetPassword(session)
            #remove token so it cant be used again
            self.interfaces.userDb.deleteToken(token)
            return JsonResponse.create(StatusCode.OK,{"email":message,"errorCode":errorCode,"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"errorCode":errorCode,"message":message})


    def changeStatus(self,system_email):
        """

        Changes status for specified user.  Associated request body should have keys 'uid' and 'new_status'

        arguments:

        system_email  -- (string) the emaily to send emails from

        return the reponse object with a success message

        """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("uid") and requestDict.exists("new_status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include uid and new_status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        # Find user that matches specified uid
        user = self.interfaces.userDb.getUserByUID(int(requestDict.getValue("uid")))

        if(user.email == None):
            return JsonResponse.error(ResponseException("User does not have a defined email",StatusCode.INTERNAL_ERROR),StatusCode.INTERNAL_ERROR)

        #check if the user is waiting
        if(self.interfaces.userDb.checkStatus(user,"awaiting_approval")):
            if(requestDict.getValue("new_status") == "approved"):
                # Grant agency_user permission to newly approved users
                self.interfaces.userDb.grantPermission(user,"agency_user")
                link=  AccountHandler.FRONT_END
                emailTemplate = { '[URL]':link,'[EMAIL]':system_email}
                newEmail = sesEmail(user.email, system_email,templateType="account_approved",parameters=emailTemplate,database=self.interfaces.userDb)
                newEmail.send()
            elif (requestDict.getValue("new_status") == "denied"):
                emailTemplate = {}
                newEmail = sesEmail(user.email, system_email,templateType="account_rejected",parameters=emailTemplate,database=self.interfaces.userDb)
                newEmail.send()
        # Change user's status
        self.interfaces.userDb.changeStatus(user,requestDict.getValue("new_status"))
        return JsonResponse.create(StatusCode.OK,{"message":"Status change successful"})

    def listUsersWithStatus(self):
        """ List all users with the specified status.  Associated request body must have key 'status' """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        try:
            users = self.interfaces.userDb.getUsersByStatus(requestDict.getValue("status"))
        except ValueError as e:
            # Client provided a bad status
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)
        userInfo = []
        for user in users:
            thisInfo = {"name":user.name, "title":user.title,  "agency":user.agency, "email":user.email, "id":user.user_id }
            userInfo.append(thisInfo)
        return JsonResponse.create(StatusCode.OK,{"users":userInfo})

    def listSubmissionsByCurrentUser(self):
        """ List all submission IDs associated with the current user ID """
        userId = LoginSession.getName(flaskSession)
        submissions = self.interfaces.jobDb.getSubmissionsByUserId(userId)
        submissionIdList = []
        for submission in submissions:
            submissionIdList.append(submission.submission_id)
        return JsonResponse.create(StatusCode.OK,{"submission_id_list": submissionIdList})

    def setNewPassword(self):
        """ Set a new password for a user, request should have keys "user_email" and "password" """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("user_email") and requestDict.exists("password"))):
            # Don't have the keys we need in request
            exc = ResponseException("Set password route requires keys user_email and password",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)

        if(not self.checkPassword(requestDict.getValue("password"))):
            exc = ResponseException("Invalid Password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user from email
        user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("user_email"))
        # Set new password
        self.interfaces.userDb.setPassword(user,requestDict.getValue("password"),self.bcrypt)

        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password successfully changed"})

    def resetPassword(self,system_email,session):
        """

        Remove old password and email user a token to set a new password.  Request should have key "email"

        arguments:

        system_email  -- (string) email used to send messages
        session  -- (Session) object from flask

        """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("email"))):
            # Don't have the keys we need in request
            exc = ResponseException("Reset password route requires key 'email'",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user object
        try:
            user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("email"))
        except Exception as e:
            exc = ResponseException("Unknown Error",StatusCode.CLIENT_ERROR,ValueError)
            return JsonResponse.error(exc,exc.status)

        LoginSession.logout(session)
        self.interfaces.userDb.session.commit()
        email = requestDict.getValue("email")
        # Send email with token
        emailToken = sesEmail.createToken(email,self.interfaces.userDb,"password_reset")
        link= "".join([ AccountHandler.FRONT_END,'/forgotpassword/',emailToken])
        emailTemplate = { '[URL]':link}
        newEmail = sesEmail(user.email, system_email,templateType="reset_password",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()
        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password reset"})

    def getCurrentUser(self,session):
        """

        Gets the current user information

        arguments:

        session  -- (Session) object from flask

        return the reponse object with the current user information

        """
        uid =  session["name"]
        user =  self.interfaces.userDb.getUserByUID(uid)
        permissionList = []
        for permission in self.interfaces.userDb.getPermssionList():
            if(self.interfaces.userDb.hasPermisson(user,permission.name)):
                permissionList.append(permission.permission_type_id)
        return JsonResponse.create(StatusCode.OK,{"user_id": int(uid),"name":user.name,"agency":user.agency,"title":user.title, "permissions" : permissionList})

    def isUserActive(self, user):
        today = parse(time.strftime("%c"))
        daysActive = (today-user.last_login_date).days
        secondsActive = (today-user.last_login_date).seconds
        if daysActive > 120 or (daysActive == 120 and secondsActive > 0):
            user.is_active = False
        self.interfaces.userDb.session.commit()
        return user.is_active
