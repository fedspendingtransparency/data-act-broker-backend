from datetime import datetime
import boto
from boto import sts
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

        """
        if(name == None):
            self.bucketRoute = CONFIG_BROKER['aws_bucket']
        else:
            self.bucketRoute = name

        s3UrlHandler.S3_ROLE = CONFIG_BROKER['aws_role']
        s3UrlHandler.REGION = CONFIG_BROKER['aws_region']


    def _signUrl(self,path,fileName,bucketRoute,method="PUT") :
        """
        Creates the object for signing URLS

        arguments:
            path - (String) path to folder
            fileName - (String) File name of file to be uploaded to S3.
            method - method to create signed url for
        returns signed url (String)

        """
        if(s3UrlHandler.ENABLE_S3) :
            s3connection = boto.s3.connect_to_region(s3UrlHandler.REGION)
            if(method=="PUT") :
                return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, method, bucketRoute, "/"+path+"/" +fileName,headers={'Content-Type': 'application/octet-stream'})
            return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, method, bucketRoute, "/"+path+"/" +fileName)
        return s3UrlHandler.BASE_URL + "/"+self.bucketRoute +"/"+path+"/" +fileName

    def getSignedUrl(self,path,fileName, bucketRoute=None, method="PUT"):
        """
        Signs a URL for PUT requests

        arguments:
        fileName -- (String) File name of file to be uploaded to S3.

        returns signed url (String)
        """
        bucketRoute = self.bucketRoute if bucketRoute is None else bucketRoute

        if(method=="PUT"):
            self.s3FileName = s3UrlHandler.getTimestampedFilename(fileName)
        else:
            self.s3FileName = fileName
        return self._signUrl(path,self.s3FileName, bucketRoute, method)

    @staticmethod
    def getTimestampedFilename(filename) :
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
        return str(seconds)+"_"+filename

    def getTemporaryCredentials(self,user):
        """
        Gets token that allows for S3 Uploads for seconds set in STS_LIFETIME
        """
        stsConnection = sts.connect_to_region(s3UrlHandler.REGION)
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
        try:
            s3UrlHandler.REGION
        except AttributeError as e:
            s3UrlHandler.REGION = CONFIG_BROKER["aws_region"]
        s3connection = boto.s3.connect_to_region(s3UrlHandler.REGION)
        bucket = s3connection.get_bucket(CONFIG_BROKER['aws_bucket'])
        key = bucket.get_key(filename)
        if key is None:
            return 0
        else:
            return key.size

    def getFileUrls(self, bucket_name, path):
        try:
            s3UrlHandler.REGION
        except AttributeError as e:
            s3UrlHandler.REGION = CONFIG_BROKER["aws_region"]

        s3connection = boto.s3.connect_to_region(s3UrlHandler.REGION)
        bucket = s3connection.get_bucket(bucket_name)

        urls = {}

        for key in bucket.list(prefix=path):
            if key.name != path:
                file_name = key.name[len(path):]
                url = self.getSignedUrl(path=path, fileName=file_name, bucketRoute=bucket_name, method="GET")
                urls[file_name] = url

        return urls
