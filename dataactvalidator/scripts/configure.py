import os
import inspect
import json

class ConfigureValidator(object):
    """

    This class creates the required json to use the Validator

    """
    @staticmethod
    def getDatacorePath():
        """Returns the dataactcore path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))[0]

    @staticmethod
    def createJSON(port,trace,debug):
        """Creates the s3bucket.json File"""
        returnJson = {}
        returnJson ["port"] = port
        returnJson ["rest_trace"] = trace
        returnJson ["server_debug"] = debug
        return json.dumps(returnJson);


    @staticmethod
    def promptWebservice():
        """Promts user validator web service"""
        debugMode = False
        traceMode = False
        reponse = raw_input("Would you like to configure your validator web service? (y/n) : ")
        if(reponse.lower() =="y"):
            port = raw_input("Enter web service port :")

            reponse = raw_input("Would you like to enable server side debuging (y/n) : ")
            if(reponse.lower() =="y"):
                debugMode = True

            reponse = raw_input("Would you like to enable debug traces on REST requests (y/n) : ")
            if(reponse.lower() =="y"):
                traceMode = True

            json = ConfigureValidator.createJSON(port,traceMode,debugMode)

            with open(ConfigureValidator.getDatacorePath()+"/validator_configuration.json", 'wb') as bucketFile :
                bucketFile.write(json)

if __name__ == '__main__':
    ConfigureValidator.promptWebservice()
