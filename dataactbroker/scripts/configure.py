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
    def createBrokerJSON(port,trace,debug,origins,enableLocalDyanmo):
        """Creates the s3bucket.json File"""
        returnJson = {}
        returnJson ["port"] = port
        returnJson ["rest_trace"] = trace
        returnJson ["server_debug"] = debug
        returnJson ["origins"] = origins
        returnJson["local_dynamo"] = enableLocalDyanmo
        returnJson["create_credentials"] = False #Local installs cant proform this action.
        return json.dumps(returnJson)


    @staticmethod
    def createValidatorJSON(url):
        """Creates the s3bucket.json File"""
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
    def promptBroker():
        """Prompts user broker api"""
        debugMode = False
        traceMode = False
        enableLocalDynamo = False
        if(ConfigureBroker.questionPrompt("Would you like to configure your broker web API? (y/n) : ")):
            port = raw_input("Enter broker API port :")

            if(ConfigureBroker.questionPrompt("Would you like to enable server side debuging (y/n) : ")):
                debugMode = True

            if(ConfigureBroker.questionPrompt("Would you like to enable debug traces on REST requests (y/n) : ")):
                traceMode = True

            origins = raw_input("Enter the allowed orgin (website that will allow for CORS) :")


            if(ConfigureBroker.questionPrompt("Would you like to use a local dyanamo database ? (y/n) : ")):
                enableLocalDynamo = True

            if(ConfigureBroker.questionPrompt("Would you like to create the dyanmo database table ? (y/n) : ")):
                SessionTable.createTable(enableLocalDynamo)

            json = ConfigureBroker.createBrokerJSON(port,traceMode,debugMode,origins,enableLocalDynamo)

            with open(ConfigureBroker.getDatacorePath()+"/web_api_configuration.json", 'wb') as configFile:
                configFile.write(json)

        if(ConfigureBroker.questionPrompt("Would you like to configure the connection to the DATA Act validator? (y/n) : ")):

            path = raw_input("Enter url (http://severurl:port) : ")
            json = ConfigureBroker.createValidatorJSON(path)
            with open(ConfigureBroker.getDatacorePath()+"/manager.json", 'wb') as configFile:
                configFile.write(json)

if __name__ == '__main__':
    promptBroker()
