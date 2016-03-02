import json
import os
import inspect
from aws.session import LoginSession
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.interfaceHolder import InterfaceHolder

class LoginHandler:
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
