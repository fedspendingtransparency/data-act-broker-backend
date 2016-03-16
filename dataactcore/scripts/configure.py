import os
import inspect
import json
from builtins import input

class ConfigureCore(object):
    """

    This class creates the required json to use the core

    """

    @staticmethod
    def getDatacorePath():
        """Returns the dataactcore path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(
            inspect.currentframe()))))[0]

    @staticmethod
    def createS3JSON(bucketName, role):
        """Creates the s3bucket.json File"""
        returnJson = {}
        returnJson["role"] = role
        returnJson["bucket"] = bucketName
        return json.dumps(returnJson)

    @staticmethod
    def createDatabaseJSON(username, password, host, port, defaultName):
        """Creates the dbCred.json File"""
        returnJson = {}
        returnJson["username"] = username
        returnJson["password"] = password
        returnJson["host"] = host
        returnJson["port"] = port
        returnJson["dbBaseName"] = defaultName
        return json.dumps(returnJson)

    @staticmethod
    def createLoggingJSON(host, port, local, path):
        """Creates the logging.json File"""
        returnJson = {}
        returnJson["host"] = host
        returnJson["port"] = port
        returnJson["local"] = local
        returnJson["local_log"] = path
        return json.dumps(returnJson)

    @staticmethod
    def questionPrompt(question):
        """Creates a yes/no question prompt"""
        response = input(question)
        if (response.lower() == "y" or response.lower() == "yes"):
            return True
        return False

    @staticmethod
    def promptS3():
        """Prompts user for input for S3 Setup"""
        if (ConfigureCore.questionPrompt(
                "Would you like to configure your S3 connection? (y/n) : ")):
            bucket = input("Enter your bucket name :")
            role = input("Enter your S3 Role :")
            json = ConfigureCore.createS3JSON(bucket, role)
            with open(ConfigureCore.getDatacorePath() + "/aws/s3bucket.json",
                      'wb') as bucketFile:
                bucketFile.write(json)

    @staticmethod
    def promptLogging():
        """Prompts user for input for S3 Setup"""
        local = False
        port = 0
        host = ""
        path = ""

        if (ConfigureCore.questionPrompt(
                "Would you like to configure your Logging? (y/n) : ")):

            if (ConfigureCore.questionPrompt(
                "Would you like to log locally (y/n) : ")):
                path = input("Enter the path for the log :")
                local = True
            else :
                port = input("Enter Port :")
                host = input("Enter the logging URL :")
            json = ConfigureCore.createLoggingJSON(host, port,local,path)
            with open(ConfigureCore.getDatacorePath() + "/utils/logging.json",
                      'wb') as currentFile:
                currentFile.write(json)

    @staticmethod
    def promptDatabase():
        """Prompts user for database setup"""
        if (ConfigureCore.questionPrompt(
                "Would you like to configure your database connection? (y/n) : ")):
            host = input("Enter your database address :")
            port = input("Enter your database port : ")
            username = input("Enter your database username : ")
            password = input("Enter your database password : ")
            databaseName = input("Enter your default database name : ")
            json = ConfigureCore.createDatabaseJSON(username, password, host,
                                                    port, databaseName)
            if (not os.path.exists(ConfigureCore.getDatacorePath() +
                                   "/credentials")):
                os.makedirs(ConfigureCore.getDatacorePath() + "/credentials")
            with open(ConfigureCore.getDatacorePath() +
                      "/credentials/dbCred.json", 'wb') as bucketFile:
                bucketFile.write(json)


if __name__ == '__main__':
    ConfigureCore.promptS3()
    ConfigureCore.promptDatabase()
    ConfigureCore.promptLogging()
