import json
import os
import inspect
from flask import session as flaskSession
from aws.session import LoginSession
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.models.userModel import UserStatus
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import LoginSession

class AccountHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    credentialFile = "credentials.json"
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

            user  = self.interfaces.userDb.getUserByEmail(username)
            if(self.interfaces.userDb.checkPassword(user,password,self.bcrypt)):
                # We have a valid login
                LoginSession.login(session,self.userManager.getUserByEmail(username).user_id)
                return JsonResponse.create(StatusCode.OK,{"message":"Login successful"})
            else :
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

    #
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
        """ Save user's information into user database.  Associated request body should have keys 'email', 'name', 'agency', and 'title' """
        requestFields = RequestDictionary(self.request)
        if(not (requestFields.exists("email") and requestFields.exists("name") and requestFields.exists("agency") and requestFields.exists("title") and requestFields.exists("password"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include email, name, agency, title, and password", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})
        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        # Add user info to database
        self.interfaces.userDb.addUserInfo(user,requestFields.getValue("name"),requestFields.getValue("agency"),requestFields.getValue("title"))
        self.interfaces.userDb.setPassword(user,requestFields.getValue("password"),self.bcrypt)
        # Send email to approver
        for user in self.interfaces.userDb.getUsersByType("website_admin") :
            emailTemplate = {'[USER]': user.name, '[USER2]':requestFields.getValue("email")}
            newEmail = sesEmail(user.email, system_email,templateType="account_creation",parameters=emailTemplate,database=self.interfaces.userDb)
            newEmail.send()
        LoginSession.logout(session)
        # Mark user as awaiting approval
        self.interfaces.userDb.changeStatus(user,"awaiting_approval")
        print("Completed registration, returning 200")
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def createEmailConfirmation(self,system_email):
        """Creates user record and email"""
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("email")):
            exc = ResponseException("Request body must include email", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})
        email = requestFields.getValue("email")
        try :
            user = self.interfaces.userDb.getUserByEmail(requestFields.getValue("email"))
        except ResponseException as e:
            self.interfaces.userDb.addUnconfirmedEmail(email)
        else:
            if(not (user.user_status_id == UserStatus.getStatus("awaiting_confirmation") or user.user_status_id == UserStatus.getStatus("email_confirmed"))):
                exc = ResponseException("User already registered", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status,{})
        emailToken = sesEmail.createToken(email,self.interfaces.userDb,"validate_email")

        link='<a href="'+AccountHandler.FRONT_END+'/check_email/'+emailToken+'">here</a>'
        emailTemplate = {'[USER]': email, '[URL]':link}
        newEmail = sesEmail(email, system_email,templateType="validate_email",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()
        return JsonResponse.create(StatusCode.OK,{"message":"Email Sent"})

    def checkEmailConfirmationToken(self,session):
        """Creates user record and email"""
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})
        token = requestFields.getValue("token")
        success,message = sesEmail.checkToken(token,self.interfaces.userDb,"validate_email")
        if(success):
            #mark session that email can be filled out
            LoginSession.register(session)
            #remove token so it cant be used again
            self.interfaces.userDb.deleteToken(token)
            #set the status
            self.interfaces.userDb.changeStatus(self.interfaces.userDb.getUserByEmail(message),"email_confirmed")
            return JsonResponse.create(StatusCode.OK,{"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"message":message})

    def checkPasswordToken(self,session):
        """"""
        requestFields = RequestDictionary(self.request)
        if(not requestFields.exists("token")):
            exc = ResponseException("Request body must include token", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})
        token = requestFields.getValue("token")
        success,message = sesEmail.checkToken(token,self.interfaces.userDb,"password_reset")
        if(success):
            #mark session that password can be filled out
            LoginSession.resetPassword(session)
            #remove token so it cant be used again
            self.interfaces.userDb.deleteToken(token)
            return JsonResponse.create(StatusCode.OK,{"message":"success"})
        else:
            #failure but alert UI of issue
            return JsonResponse.create(StatusCode.OK,{"message":message})

    def changeStatus(self):
        """ Changes status for specified user.  Associated request body should have keys 'user_email' and 'new_status' """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("user_email") and requestDict.exists("new_status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include user_email and new_status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})

        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("user_email"))

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
            thisInfo = {"name":user.name, "email":user.email, "agency":user.agency, "title":user.title}
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
        # Get user from email
        user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("user_email"))
        # Set new password
        self.interfaces.userDb.setPassword(user,requestDict.getValue("password"))
        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password successfully changed"})

    def resetPassword(self,system_email):
        """ Remove old password and email user a token to set a new password.  Request should have key "email" """
        requestDict = RequestDictionary(self.request)
        if(not (requestDict.exists("email"))):
            # Don't have the keys we need in request
            exc = ResponseException("Reset password route requires key 'email'",StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        # Get user object
        user = self.interfaces.userDb.getUserByEmail(requestDict.getValue("email"))
        # Remove current password hash
        user.password_hash = None
        self.interfaces.userDb.session.commit()
        email = requestDict.getValue("email")
        # Send email with token
        emailToken = sesEmail.createToken(email,self.interfaces.userDb,"password_reset")

        link='<a href="'+AccountHandler.FRONT_END+'/passwordreset/'+emailToken+'">here</a>'
        emailTemplate = {'[USER]': email, '[URL]':link}
        newEmail = sesEmail(user.email, system_email,templateType="reset_password",parameters=emailTemplate,database=self.interfaces.userDb)
        newEmail.send()
        # Return success message
        return JsonResponse.create(StatusCode.OK,{"message":"Password reset"})
