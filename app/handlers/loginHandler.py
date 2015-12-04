import json
import os

from json import JSONDecoder, JSONEncoder
from aws.session import LoginSession
from utils.requestDictionary import RequestDictionary

class LoginHandler:
    # Handles login process, compares username and password provided
    credentialFile = "credentials.json"

    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request,response):
        # Set Http request and response objects
        self.request = request
        self.response = response

        response.headers.add("Content-Type","application/json")

    def login(self,session):
        self.response.headers["Content-Type"] = "application/json"
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
                LoginSession.login(session,username)
                self.response.status_code = 200
                self.response.set_data(json.dumps({"message":"Login successful"}))
                return self.response


        except (TypeError, KeyError, NotImplementedError) as e:
            # Return a 400 with appropriate message
            self.response.status_code = 400
            self.response.set_data(json.dumps({"message":(e.message+","+str(self.request.get_json))}))
        except ValueError as e:
            # Return a 401 for login denied
            self.response.status_code = 401
            self.response.set_data(json.dumps({"message":(e.message)}))
        return self.response

    # This function removes the session from the session table if currently logged in, and then returns a success message
    def logout(self,session):
        self.response.headers["Content-Type"] = "application/json"
        # Call session handler
        LoginSession.logout(session)
        self.response.status_code = 200
        self.response.set_data(json.dumps({"message":"Logout successful"}))
        return self.response
