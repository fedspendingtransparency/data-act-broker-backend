import os
import inspect
import json
import sys
from builtins import input
from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.scripts.configure import ConfigureCore
from dataactbroker.handlers.aws.session import SessionTable
from dataactbroker.handlers.userHandler import UserHandler
from flask.ext.bcrypt import Bcrypt

class ConfigureBroker(object):
    """

    This class creates the required json to use the broker

    """
    @staticmethod
    def getDataBrokerPath():
        """Returns the dataactbroker path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))[0]

    @staticmethod
    def createTestUsers(adminEmail,adminpass,enableTestUsers):
        """Creates Tests Users"""
        crypt = Bcrypt()
        userDatabase = UserHandler()
        if(enableTestUsers):
            userDatabase.createUser("user","bestPassEver" ,crypt)
            userDatabase.createUser("user2","NotAPassword" ,crypt)
            userDatabase.createUser("user3","123abc" ,crypt)
        userDatabase.createUser(adminEmail,adminpass ,crypt,admin=True)


    @staticmethod
    def createBrokerJSON(localInstall,localFolder,port,trace,debug,origins,enableLocalDyanmo,localDynamoPort,emailAddress,frontendURL,key):
        """Creates the web_api_configuration.json File"""
        returnJson = {}
        returnJson ["local"] = localInstall
        returnJson ["local_folder"] = localFolder
        returnJson ["port"] = port
        returnJson ["dynamo_port"] = localDynamoPort
        returnJson ["rest_trace"] = trace
        returnJson ["server_debug"] = debug
        returnJson ["origins"] = origins
        returnJson["local_dynamo"] = enableLocalDyanmo
        returnJson["create_credentials"] = True
        returnJson["system_email"] = emailAddress
        returnJson["frontend_url"] = frontendURL
        returnJson["security_key"] = key
        return json.dumps(returnJson)


    @staticmethod
    def createValidatorJSON(url):
        """Creates the manager.json File"""
        returnJson = {}
        returnJson ["URL"] = url
        return json.dumps(returnJson)


    @staticmethod
    def questionPrompt(question):
        "Creates a yes/no question prompt"
        response = input(question)
        if(response.lower() =="y" or response.lower() =="yes" ):
            return True
        return False

    @staticmethod
    def createFile(filename,json):
        """Creates a file with the json and filename"""
        with open(ConfigureBroker.getDataBrokerPath()+filename, 'wb') as configFile:
            configFile.write(json)

    @staticmethod
    def promptBroker():
        """Prompts user broker api"""
        # Create the config directory
        if (not os.path.exists("".join([ConfigureBroker.getDataBrokerPath(), "/config"]))):
                os.makedirs("".join([ConfigureBroker.getDataBrokerPath(), "/config"]))


        if(ConfigureBroker.questionPrompt("Would you like to configure your broker web API? (y/n) : ")):
            localFolder = ""
            localInstall =  ConfigureBroker.questionPrompt("Would you like to install the broker locally? (y/n): ")

            if(localInstall):
                localFolder =  input("Enter the local folder used by the broker : ")

            debugMode = False
            traceMode = False
            enableLocalDynamo = False


            port = input("Enter broker API port :")
            try:
                int(port)
            except ValueError:
                print ("Invalid Port")
                return

            if(ConfigureBroker.questionPrompt("Would you like to enable server side debugging (y/n) : ")):
                debugMode = True

            if(ConfigureBroker.questionPrompt("Would you like to enable debug traces on REST requests (y/n) : ")):
                traceMode = True

            origins = input("Enter the allowed origin (website that will allow for CORS) :")

            emailAddress = input("Enter System Email Address :")

            localPort  = 8000
            if( localInstall or ConfigureBroker.questionPrompt("Would you like to use a local dynamo database ? (y/n) : ")):
                enableLocalDynamo = True
                localPort = input("Enter the port for the local dynamo database : ")
                try:
                    localPort =  int(localPort)
                except ValueError:
                    print ("Invalid Port")
                    return

            frontend = input("Enter the URL for the React application: ")

            key = input("Enter application security Key: ")

            if(ConfigureBroker.questionPrompt("Would you like to create the dyanmo database table ? (y/n) : ")):
                SessionTable.createTable(enableLocalDynamo,localPort)

            json = ConfigureBroker.createBrokerJSON(localInstall,localFolder,port,traceMode,debugMode,origins,enableLocalDynamo,localPort,emailAddress,frontend,key)

            ConfigureBroker.createFile("/config/web_api_configuration.json",json)
        if(ConfigureBroker.questionPrompt("Would you like to configure the connection to the DATA Act validator? (y/n) : ")):

            path = input("Enter url (http://severurl:port) : ")
            json = ConfigureBroker.createValidatorJSON(path)

            ConfigureBroker.createFile("/config/manager.json",json)
        if(ConfigureBroker.questionPrompt("Would you like to configure the users to the DATA Act web api? (y/n) : ")):
            testCaseUsers = False
            if(ConfigureBroker.questionPrompt("Would you like to include test case users (y/n) : ")):
                testCaseUsers = True
            password = input("Enter the admin user password:")
            adminEmail = input("Enter the admin user email:")
            ConfigureBroker.createTestUsers(adminEmail,password,testCaseUsers)

if __name__ == '__main__':
    ConfigureBroker.promptBroker()
