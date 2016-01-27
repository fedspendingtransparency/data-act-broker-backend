import os
import inspect
import json

class ConfigureCore(object):
    """

    This class creates the required json to use the core

    """
    @staticmethod
    def getDatacorePath():
        """Returns the dataactcore path based on install location"""
        return os.path.split(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))[0]

    @staticmethod
    def createS3JSON(bucketName,role):
        """Creates the s3bucket.json File"""
        returnJson = {}
        returnJson ["role"] = role
        returnJson ["bucket"] = bucketName
        return json.dumps(returnJson);

    @staticmethod
    def createDatabaseJSON(username,password,host,port,defaultName):
        """Creates the dbCred.json File"""
        returnJson = {}
        returnJson ["username"] =username
        returnJson ["password"] =password
        returnJson ["host"] = host
        returnJson ["port"] = port
        returnJson ["dbBaseName"] = defaultName
        return json.dumps(returnJson);

    @staticmethod
    def promtS3():
        """Promts user for input for S3 Setup"""
        reponse = raw_input("Would you like to configure your S3 connection? (y/n) : ")
        if(reponse.lower() =="y"):
            bucket = raw_input("Enter your bucket name :")
            role = raw_input("Eneter your S3 Role :")
            json = ConfigureCore.createS3JSON(bucket,role)
            with open(ConfigureCore.getDatacorePath()+"/aws/s3bucket.json", 'wb') as bucketFile :
                bucketFile.write(json)


    @staticmethod
    def promtDatabase():
        """Promts user for database setup"""
        reponse = raw_input("Would you like to configure your database connection? (y/n) : ")
        if(reponse.lower() =="y"):
            host = raw_input("Enter your database address :")
            port = raw_input("Enter your database port : ")
            username = raw_input("Enter your database username : ")
            password = raw_input("Enter your database password : ")
            databaseName = raw_input("Enter your default database name : ")
            json = ConfigureCore.createDatabaseJSON(username,password,host,port,databaseName)
            with open(ConfigureCore.getDatacorePath()+"/credentials/dbCred.json", 'wb') as bucketFile :
                bucketFile.write(json)

if __name__ == '__main__':
    ConfigureCore.promtS3()
    ConfigureCore.promtDatabase()
