import json
import os
import inspect
from flask import session as flaskSession
from aws.session import LoginSession
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.interfaceHolder import InterfaceHolder

class AccountHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    credentialFile = "credentials.json"

    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request,interfaces):
        """

        Creates the Login Handler
        """
        self.userManager = interfaces.userDb
        self.request = request
        self.interfaces = interfaces

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

            # For now import credentials list from a JSON file
            path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
            lastBackSlash = path.rfind("\\",0,-1)
            lastForwardSlash = path.rfind("/",0,-1)
            lastSlash = max([lastBackSlash,lastForwardSlash])
            credFile = path[0:lastSlash] + "/" + self.credentialFile
            credJson = open(credFile,"r").read()


            credDict = json.loads(credJson)


            # Check for valid username and password
            if(not(username in credDict)):
                raise ValueError("Not a recognized user")
            elif(credDict[username] != password):
                raise ValueError("Incorrect password")
            else:
                # We have a valid login
                LoginSession.login(session,self.userManager.getUserId(username))
                return JsonResponse.create(StatusCode.OK,{"message":"Login successful"})

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

    def register(self):
        """ Save user's information into user database.  Associated request body should have keys 'email', 'name', 'agency', and 'title' """
        input = RequestDictionary(self.request)
        if(not (input.exists("email") and input.exists("name") and input.exists("agency") and input.exists("title"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include email, name, agency, and title", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})
        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(input.getValue("email"))
        # Add user info to database
        self.interfaces.userDb.addUserInfo(user,input.getValue("name"),input.getValue("agency"),input.getValue("title"))
        # Send email to approver
        # TODO implement
        # Mark user as awaiting approval
        self.interfaces.userDb.changeStatus(user,"awaiting_approval")
        return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})

    def changeStatus(self):
        """ Changes status for specified user.  Associated request body should have keys 'user_email' and 'new_status' """
        input = RequestDictionary(self.request)
        if(not (input.exists("user_email") and input.exists("new_status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include user_email and new_status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status,{})

        # Find user that matches specified email
        user = self.interfaces.userDb.getUserByEmail(input.getValue("user_email"))

        # Change user's status
        self.interfaces.userDb.changeStatus(user,input.getValue("new_status"))
        return JsonResponse.create(StatusCode.OK,{"message":"Status change successful"})

    def listUsersWithStatus(self):
        """ List all users with the specified status.  Associated request body must have key 'status' """
        input = RequestDictionary(self.request)
        if(not (input.exists("status"))):
            # Missing a required field, return 400
            exc = ResponseException("Request body must include status", StatusCode.CLIENT_ERROR)
            return JsonResponse.error(exc,exc.status)
        try:
            users = self.interfaces.userDb.getUsersByStatus(input.getValue("status"))
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