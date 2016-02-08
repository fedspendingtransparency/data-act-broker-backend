import os
import inspect
import json
import sys

from dataactcore.scripts.databaseSetup import runCommands
from dataactbroker.handlers.aws.session import SessionTable

class ConfigureBroker(object):
    """

    This class creates the required json to use the broker

    """
    @staticmethod
    def getDatacorePath():
        """Returns the dataactcore path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))[0]

    @staticmethod
    def createLoginJSON(adminpass,enableTestUsers):
        """Creates the credentials.json File"""
        returnJson = {}
        if(enableTestUsers):
            returnJson ["user"] = "bestPassEver"
            returnJson ["user2"] = "NotAPassword"
            returnJson ["user3"] = "123abc"
        returnJson["admin"] = adminpass
        return json.dumps(returnJson)

    @staticmethod
    def createBrokerJSON(port,trace,debug,origins,enableLocalDyanmo,localDynamoPort):
        """Creates the web_api_configuration.json File"""
        returnJson = {}
        returnJson ["port"] = port
        returnJson ["dynamo_port"] = localDynamoPort
        returnJson ["rest_trace"] = trace
        returnJson ["server_debug"] = debug
        returnJson ["origins"] = origins
        returnJson["local_dynamo"] = enableLocalDyanmo
        returnJson["create_credentials"] = True
        return json.dumps(returnJson)


    @staticmethod
    def createValidatorJSON(url):
        """Creates the manager.json File"""
        returnJson = {}
        returnJson ["URL"] = url
        return json.dumps(returnJson)


    @staticmethod
    def questionPrompt(question):
        "Creates a yes/no question propt"
        response = raw_input(question)
        if(response.lower() =="y" or response.lower() =="yes" ):
            return True
        return False

    @staticmethod
    def createFile(filename,json):
        """"""
        with open(ConfigureBroker.getDatacorePath()+filename, 'wb') as configFile:
            configFile.write(json)

    @staticmethod
    def promptBroker():
        """Prompts user broker api"""
        debugMode = False
        traceMode = False
        enableLocalDynamo = False
        if(ConfigureBroker.questionPrompt("Would you like to configure your broker web API? (y/n) : ")):
            port = raw_input("Enter broker API port :")
            try:
                int(port)
            except ValueError:
                print ("Invalid Port")
                return

            if(ConfigureBroker.questionPrompt("Would you like to enable server side debuging (y/n) : ")):
                debugMode = True

            if(ConfigureBroker.questionPrompt("Would you like to enable debug traces on REST requests (y/n) : ")):
                traceMode = True

            origins = raw_input("Enter the allowed orgin (website that will allow for CORS) :")

            localPort  = 8000
            if(ConfigureBroker.questionPrompt("Would you like to use a local dyanamo database ? (y/n) : ")):
                enableLocalDynamo = True
                localPort = raw_input("Enter the port for the local DynamoDB : ")
                try:
                    localPort =  int(localPort)
                except ValueError:
                    print ("Invalid Port")
                    return

            if(ConfigureBroker.questionPrompt("Would you like to create the dyanmo database table ? (y/n) : ")):
                SessionTable.createTable(enableLocalDynamo,localPort)

            json = ConfigureBroker.createBrokerJSON(port,traceMode,debugMode,origins,enableLocalDynamo,localPort)

            ConfigureBroker.createFile("/web_api_configuration.json",json)
        if(ConfigureBroker.questionPrompt("Would you like to configure the connection to the DATA Act validator? (y/n) : ")):

            path = raw_input("Enter url (http://severurl:port) : ")
            json = ConfigureBroker.createValidatorJSON(path)

            ConfigureBroker.createFile("/manager.json",json)
        if(ConfigureBroker.questionPrompt("Would you like to configure the users to the DATA Act web api? (y/n) : ")):
            testCaseUsers = False
            if(ConfigureBroker.questionPrompt("Would you like to include test case users (y/n) : ")):
                testCaseUsers = True
            password = raw_input("Enter the admin user password:")
            json = ConfigureBroker.createLoginJSON(password,testCaseUsers)
            ConfigureBroker.createFile("/credentials.json",json)

if __name__ == '__main__':
    promptBroker()
