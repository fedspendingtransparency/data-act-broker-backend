import os
import inspect
import json
import sys
from builtins import input
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.filestreaming.tasLoader import TASLoader
from dataactvalidator.scripts.setupTASIndexs import setupTASIndexs

class ConfigureValidator(object):
    """

    This class creates the required json to use the Validator

    """
    @staticmethod
    def getDatacorePath():
        """Returns the dataactcore path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))[0]

    @staticmethod
    def createJSON(port,trace,debug,host):
        """Creates the s3bucket.json File"""
        returnJson = {}
        returnJson ["port"] = port
        returnJson ["rest_trace"] = trace
        returnJson ["server_debug"] = debug
        returnJson ["host"] = host
        return json.dumps(returnJson)

    @staticmethod
    def questionPrompt(question):
        "Creates a yes/no question propt"
        response = input(question)
        if(response.lower() =="y" or response.lower() =="yes" ):
            return True
        return False


    @staticmethod
    def promptForAppropriations():
        validInput = False
        while((not validInput) and ConfigureValidator.questionPrompt("Would you like to configure your appropriations rules? (y/n) : ")):
            path = input("Enter the full file path for your schema (appropriationsFields.csv) : " ).strip()
            try :
                SchemaLoader.loadFields("appropriations",path)
                validInput = True
            except IOError as e:
                print("Cant open file")
            except Exception as e:
                  print("Unexpected error:", sys.exc_info()[0])
            path = input("Enter the full file path for your rules (appropriationsRules.csv) :  " ).strip()

            try :
                SchemaLoader.loadRules("appropriations",path)
                validInput = True
            except IOError as e:
                print("Cant open file")
            except Exception as e:
                  print("Unexpected error:", sys.exc_info()[0])
    @staticmethod
    def promptForTAS():
        validInput = False
        while((not validInput) and ConfigureValidator.questionPrompt("Would you like to add a new TAS File? (y/n) : ")):

            path = input("Enter the full file path for your TAS data (all_tas_betc.csv) : " ).strip()
            try :
                TASLoader.loadFields(path)
                setupTASIndexs()
                validInput = True
            except IOError as e:
                print("Cant open file")
            except Exception as e:
                 print("Unexpected error:", sys.exc_info()[0])
    @staticmethod
    def promptWebservice():
        """Promts user validator web service"""
        debugMode = False
        traceMode = False
        if(ConfigureValidator.questionPrompt("Would you like to configure your validator web service? (y/n) : ")):
            port = input("Enter web service port :")

            host = input("Enter the host : ")

            if(ConfigureValidator.questionPrompt("Would you like to enable server side debuging (y/n) : ")):
                debugMode = True

            if(ConfigureValidator.questionPrompt("Would you like to enable debug traces on REST requests (y/n) : ")):
                traceMode = True

            json = ConfigureValidator.createJSON(port,traceMode,debugMode,host)

            with open("".join([ConfigureValidator.getDatacorePath(),"/validator_configuration.json"]), 'wb') as bucketFile:
                bucketFile.write(json)

if __name__ == '__main__':
    ConfigureValidator.promptWebservice()
    ConfigureValidator.promptForAppropriations()
    ConfigureValidator.promptForTAS()
