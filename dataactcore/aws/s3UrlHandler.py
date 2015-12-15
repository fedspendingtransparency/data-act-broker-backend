from datetime import datetime, timedelta
import boto
import os
import inspect
import json

class s3UrlHandler:
    """
    This class acts a wrapper for S3 URL Signing
    """
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = True
    URL_LIFETIME = 2000

    def __init__(self,name,user):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket
        user -- (int) User id folder of S3 bucket

        """
        self.bucketRoute = name
        self.user  = user

    def _signUrl(self,fileName) :
        """
        Creates the object for signing URLS

        arguments:

        fileName -- (String) File name of file to be uploaded to S3.

        returns signed url (String)

        """
        if(s3UrlHandler.ENABLE_S3) :
            s3connection = boto.connect_s3()
            return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, 'PUT', self.bucketRoute, "/"+str(self.user)+"/" +fileName)
        return s3UrlHandler.BASE_URL + "/"+self.bucketRoute +"/"+self.user+"/" +fileName

    def getSignedUrl(self,fileName):
        """
        Signs a URL for PUT requests

        arguments:
        fileName -- (String) File name of file to be uploaded to S3.

        returns signed url (String)
        """
        seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
        self.s3FileName = str(seconds)+"_"+fileName
        return self._signUrl(self.s3FileName)

    @staticmethod
    def getBucketNameFromConfig():
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        bucketFile = open(path+"/s3bucket.json","r").read()
        bucketDict = json.loads(bucketFile)
        return bucketDict["bucket"]