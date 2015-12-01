import json
import os

from json import JSONDecoder, JSONEncoder
from aws.session import LoginSession


class LoginHandler:
    # Handles login process, compares username and password provided
    credentialFile = "credentials.json"

    # Instance fields include request, response, logFlag, and logFile

    def __init__(self,request,response):
        # Set Http request and response objects
        self.request = request
        self.response = response
        # Set logFlag to true if you want a log file
        self.logFlag = True
        if(self.logFlag):
            self.logFile = open("logFile.dat","w")

        response.headers.add("Content-Type","application/json")

    def login(self,session):
        print("Login function")
        try:
            self.response.headers["Content-Type"] = "application/json"
            if(self.logFlag):
                self.logFile.write(str(self.request))
                self.logFile.write(self.request.headers['Content-Type'])
            print("Checking header")
            if((self.request.headers['Content-Type'] == "application/json")):
                # Get the JSON out of the request
                print("Getting json")
                loginDict = self.request.get_json()
            elif((self.request.headers['Content-Type'] == "application/x-www-form-urlencoded")):


                print("Hit form urlencoded")
                print(self.request.form)
                loginDict = self.request.form
                #raise NotImplementedError("Url encoded not implemented yet")
            else:
                raise ValueError("Must pass in json or urlencoded form")




            if(self.logFlag):
                self.logFile.write(str(loginDict)+"\n")
            if(not(isinstance(loginDict,dict))):
                raise TypeError("Failed to create a dictionary out of json")
            # Make sure username and password are present
            if(not('username' in loginDict)):
                raise KeyError("Missing username")
            elif(not('password' in loginDict)):
                raise KeyError("Missing password")
            username = loginDict['username']
            if(self.logFlag):
                self.logFile.write("Loaded username"+"\n")
            password = loginDict['password']
            if(self.logFlag):
                self.logFile.write("Loaded password"+"\n")
            # For now import credentials list from a JSON file
            credJson = open(os.getcwd()+"/"+self.credentialFile,"r").read()
            if(self.logFlag):
                self.logFile.write(credJson+"\n")
                self.logFile.write(str(type(credJson))+"\n")

            credDict = json.loads(credJson)
            if(self.logFlag):
                self.logFile.write(str(type(credDict))+"\n")
                self.logFile.write(str(credDict)+"\n")
                self.logFile.write("Checking for:"+"\n")
                self.logFile.write(username+"\n")
                self.logFile.write(password+"\n")
                self.logFile.write("Checking username and password"+"\n")

            # Check for valid username and password
            if(not(username in credDict)):
                if(self.logFlag):
                    self.logFile.write("Bad username"+"\n")
                raise ValueError("Not a recognized user")
            elif(credDict[username] != password):
                if(self.logFlag):
                    self.logFile.write("Bad password"+"\n")
                raise ValueError("Incorrect password")
            else:
                # We have a valid login
                if(self.logFlag):
                    self.logFile.write("Valid login"+"\n")
                LoginSession.login(session)
                self.response.status_code = 200
                self.response.set_data(json.dumps({"message":"Login successful"}))
                return self.response


        except (TypeError, KeyError, ValueError, NotImplementedError) as e:
            # Return a 400 with appropriate message
            if(self.logFlag):
                self.logFile.write(str(type(e))+"\n")
            self.response.status_code = 400
            self.response.set_data(json.dumps({"message":(e.message+","+str(self.request.get_json))}))
        return self.response

    # This function removes the session from the session table if currently logged in, and then returns a success message
    def logout(self,session):
        self.response.headers["Content-Type"] = "application/json"
        # TODO: Add calls to session handler to check for session and then remove it
        LoginSession.logout(session)
        self.response.status_code = 200
        self.response.set_data(json.dumps({"message":"Logout successful"}))
        return self.response
