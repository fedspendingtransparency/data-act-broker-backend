from datetime import datetime
import boto
import os
import inspect
import json
from dataactcore.config import CONFIG_BROKER

class s3UrlHandler:
    """
    This class acts a wrapper for S3 URL Signing
    """
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = True
    URL_LIFETIME = 2000
    STS_LIFETIME = 2000
    S3_ROLE = ""
    def __init__(self,name = None):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket
        user -- (int) User id folder of S3 bucket

        """
        if(name == None):
            self.bucketRoute = CONFIG_BROKER['aws_bucket']
        else:
            self.bucketRoute = name
        s3UrlHandler.S3_ROLE = CONFIG_BROKER['aws_role']

    def _signUrl(self,path,fileName,method="PUT") :
        """
        Creates the object for signing URLS

        arguments:

        fileName -- (String) File name of file to be uploaded to S3.

        returns signed url (String)

        """
        if(s3UrlHandler.ENABLE_S3) :
            s3connection = boto.connect_s3()
            if(method=="PUT") :
                return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, method, self.bucketRoute, "/"+path+"/" +fileName,headers={'Content-Type': 'application/octet-stream'})
            return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, method, self.bucketRoute, "/"+path+"/" +fileName)
        return s3UrlHandler.BASE_URL + "/"+self.bucketRoute +"/"+path+"/" +fileName

    def getSignedUrl(self,path,fileName,method="PUT"):
        """
        Signs a URL for PUT requests

        arguments:
        fileName -- (String) File name of file to be uploaded to S3.

        returns signed url (String)
        """
        if(method=="PUT"):
            self.s3FileName = s3UrlHandler.getTimeStampedFilename(fileName)
        else:
            self.s3FileName = fileName
        return self._signUrl(path,self.s3FileName, method)

    @staticmethod
    def getTimestampedFilename(filename) :
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
        return str(seconds)+"_"+filename

    @staticmethod
    def doesFileExist(filename):
        """ Returns True if specified filename exists in the S3 bucket """
        # Get key
        s3connection = boto.connect_s3()
        bucket = s3connection.get_bucket(CONFIG_BROKER['aws_bucket'])
        key = bucket.get_key(filename)
        if key:
            return True
        else:
            return False

    def getTemporaryCredentials(self,user):
        """
        Gets token that allows for S3 Uploads for seconds set in STS_LIFETIME
        """
        stsConnection = boto.connect_sts()
        role = stsConnection.assume_role(s3UrlHandler.S3_ROLE,"FileUpload"+str(user),duration_seconds=s3UrlHandler.STS_LIFETIME)
        credentials ={}
        credentials["AccessKeyId"] =  role.credentials.access_key
        credentials["SecretAccessKey"] = role.credentials.secret_key
        credentials["SessionToken"] = role.credentials.session_token
        credentials["Expiration"] = role.credentials.expiration
        return credentials

    @staticmethod
    def getFileSize(filename):
        """ Returns file size in number of bytes for specified filename, or False if file doesn't exist """

        # Get key
        s3connection = boto.connect_s3()
        bucket = s3connection.get_bucket(CONFIG_BROKER['aws_bucket'])
        key = bucket.get_key(filename)
        if(key == None):
            return False
        else:
            return key.size
