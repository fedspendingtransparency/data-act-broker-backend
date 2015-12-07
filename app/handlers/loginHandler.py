import json
import os

from json import JSONDecoder, JSONEncoder
from aws.session import LoginSession
from utils.requestDictionary import RequestDictionary
from userHandler import UserHandler
from utils.jsonResponse import JsonResponse
from utils.statusCode import StatusCode
class LoginHandler:
    """
    This class contains the login / logout  functions
    """
    # Handles login process, compares username and password provided
    credentialFile = "credentials.json"

    # Instance fields include request, response, logFlag, and logFile

    def __init__(self):
        """

        Creates the Login Handler
        """
        self.userManager = UserHandler()
        self.request = None

    def setRequest(self, request):
        """ Set Http request object

        Arguments:
        request  -- (Request) object from flask
        """
        self.request = request

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
            credJson = open(os.getcwd()+"/"+self.credentialFile,"r").read()


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
            return JsonResponse.error(e,StatusCode.ERROR)
        except ValueError as e:
            # Return a 401 for login denied
            return JsonResponse.error(e,StatusCode.LOGIN_REQUIRED)
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
